#ifdef DIRECTPATH

#include <base/log.h>
#include <base/time.h>
#include <base/thread.h>

#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>

#include "../defs.h"
#include "mlx5_ifc.h"
#include <infiniband/mlx5dv.h>
#include <util/udma_barrier.h>
#include <util/mmio.h>

#include "defs.h"

static struct mlx5dv_devx_obj *root_flow_tbl;
static struct mlx5dv_devx_obj *root_flow_group;

static uint32_t table_number;
static uint32_t flow_group_number;

static off_t bar_offs;
static size_t bar_map_size;
static int bar_fd;

/* flow steering stuff */
#define FLOW_TBL_TYPE 0x0
#define FLOW_TBL_LOG_ENTRIES 12
#define FLOW_TBL_NR_ENTRIES (1 << FLOW_TBL_LOG_ENTRIES)
static DEFINE_BITMAP(mac_used_entries, FLOW_TBL_NR_ENTRIES);

#define TIME_OP(opn, x) do { \
	uint64_t t = rdtsc(); \
	barrier(); \
	x; \
	barrier(); \
	uint64_t a = rdtsc(); \
	log_err("%s took %0.2f micros", opn, (double)(a - t) / (double)cycles_per_us); \
	} while (0);


struct ibv_context *vfcontext;
struct cq *cqn_to_cq_map[MAX_CQ];

static unsigned char rss_key[40] = {
	0x82, 0x19, 0xFA, 0x80, 0xA4, 0x31, 0x06, 0x59, 0x3E, 0x3F, 0x9A,
	0xAC, 0x3D, 0xAE, 0xD6, 0xD9, 0xF5, 0xFC, 0x0C, 0x63, 0x94, 0xBF,
	0x8F, 0xDE, 0xD2, 0xC5, 0xE2, 0x04, 0xB1, 0xCF, 0xB1, 0xB1, 0xA1,
	0x0D, 0x6D, 0x86, 0xBA, 0x61, 0x78, 0xEB};

static void cq_fill_spec(struct shm_region *reg,
	                      struct cq *cq, struct directpath_ring_q_spec *spec)
{
	spec->stride = sizeof(struct mlx5_cqe64);
	spec->nr_entries = cq->cqe_cnt;
	spec->buf = ptr_to_shmptr(reg, cq->buf, spec->stride * spec->nr_entries);
	spec->dbrec = ptr_to_shmptr(reg, cq->dbrec, CACHE_LINE_SIZE);
}

static void wq_fill_spec(struct shm_region *reg,
	                      struct wq *wq, struct directpath_ring_q_spec *spec)
{
	spec->stride = wq->stride;
	spec->nr_entries = wq->wqe_cnt;
	spec->buf = ptr_to_shmptr(reg, wq->buf, spec->stride * spec->nr_entries);
	spec->dbrec = ptr_to_shmptr(reg, wq->dbrec, CACHE_LINE_SIZE);
}

static void qp_fill_spec(struct shm_region *reg, struct qp *qp,
	                       struct directpath_queue_spec *spec)
{
	spec->sqn = qp->sqn;
	spec->uarn = qp->uarn;
	spec->uar_offset = qp->uar_offset;
	wq_fill_spec(reg, &qp->rx_wq, &spec->rx_wq);
	cq_fill_spec(reg, &qp->rx_cq, &spec->rx_cq);
	wq_fill_spec(reg, &qp->tx_wq, &spec->tx_wq);
	cq_fill_spec(reg, &qp->tx_cq, &spec->tx_cq);
}

static void qp_fill_iokspec(struct thread *th, struct qp *qp)
{
	struct hwq *h = &th->directpath_hwq;

	h->descriptor_table = qp->rx_cq.buf;
	h->descriptor_log_size = __builtin_ctzl(sizeof(struct mlx5_cqe64));
	h->nr_descriptors = 1UL << DEFAULT_CQ_LOG_SZ;
	h->parity_byte_offset = offsetof(struct mlx5_cqe64, op_own);
	h->parity_bit_mask = MLX5_CQE_OWNER_MASK;
	h->hwq_type = HWQ_MLX5;
	h->enabled = true;
	h->consumer_idx = &th->q_ptrs->directpath_rx_tail;
}

static int directpath_activate_rx(struct directpath_ctx *ctx)
{
	uint32_t in[DEVX_ST_SZ_DW(set_fte_in) + DEVX_ST_SZ_DW(dest_format)] = {};
	uint32_t out[DEVX_ST_SZ_DW(set_fte_out)] = {};
	void *in_flow_context;
	uint8_t *in_dests;
	int index;

	index = bitmap_find_next_cleared(mac_used_entries, FLOW_TBL_NR_ENTRIES, 0);
	if (index == FLOW_TBL_NR_ENTRIES) {
		log_err("flow table exhausted!");
		return -ENOMEM;
	}

	DEVX_SET(set_fte_in, in, opcode, MLX5_CMD_OP_SET_FLOW_TABLE_ENTRY);
	DEVX_SET(set_fte_in, in, table_type, FLOW_TBL_TYPE);
	DEVX_SET(set_fte_in, in, table_id, table_number);
	DEVX_SET(set_fte_in, in, flow_index, index);

	in_flow_context = DEVX_ADDR_OF(set_fte_in, in, flow_context);
	DEVX_SET(flow_context, in_flow_context, group_id, flow_group_number);
	DEVX_SET(flow_context, in_flow_context, action, (1 << 2));

	memcpy(DEVX_ADDR_OF(flow_context, in_flow_context, match_value.outer_headers.dmac_47_16), &ctx->p->mac, sizeof(ctx->p->mac));

	DEVX_SET(flow_context, in_flow_context, destination_list_size, 1);

	in_dests = DEVX_ADDR_OF(flow_context, in_flow_context, destination);
	DEVX_SET(dest_format, in_dests, destination_type, MLX5_FLOW_DEST_TYPE_TIR);
	DEVX_SET(dest_format, in_dests, destination_id, ctx->tirn);

	ctx->fte = mlx5dv_devx_obj_create(vfcontext, in, sizeof(in), out, sizeof(out));

	if (!ctx->fte) {
		log_err("couldnt create STE for tir");
		return -1;
	}

	bitmap_set(mac_used_entries, index);
	ctx->flow_tbl_index = index;

	return 0;
}

static int setup_steering(void)
{
	uint32_t in[DEVX_ST_SZ_DW(create_flow_table_in)] = {0};
	uint32_t out[DEVX_ST_SZ_DW(create_flow_table_out)] = {0};

	uint32_t infg[DEVX_ST_SZ_DW(create_flow_group_in)] = {0};
	uint32_t outfg[DEVX_ST_SZ_DW(create_flow_group_out)] = {0};

	void *ftc;

	DEVX_SET(create_flow_table_in, in, opcode, MLX5_CMD_OP_CREATE_FLOW_TABLE);

	DEVX_SET(create_flow_table_in, in, table_type, FLOW_TBL_TYPE /* NIC RX */); // 0x4 /* ESW FDB */); // todo?
	ftc = DEVX_ADDR_OF(create_flow_table_in, in, flow_table_context);

	DEVX_SET(flow_table_context, ftc, table_miss_action, 0); // ??
	DEVX_SET(flow_table_context, ftc, level, 0);
	DEVX_SET(flow_table_context, ftc, log_size, 12);

	root_flow_tbl = mlx5dv_devx_obj_create(vfcontext, in, sizeof(in), out, sizeof(out));
	if (!root_flow_tbl) {
		LOG_CMD_FAIL("failed to create root flow tbl", create_flow_table_out, out);
		return -1;
	}

	table_number = DEVX_GET(create_flow_table_out, out, table_id);

	DEVX_SET(create_flow_group_in, infg, opcode, MLX5_CMD_OP_CREATE_FLOW_GROUP);
	DEVX_SET(create_flow_group_in, infg, table_type, FLOW_TBL_TYPE); //0x4
	DEVX_SET(create_flow_group_in, infg, table_id, table_number);
	DEVX_SET(create_flow_group_in, infg, start_flow_index, 0);
	DEVX_SET(create_flow_group_in, infg, end_flow_index, (1 << 12) - 1);

	DEVX_SET(create_flow_group_in, infg, match_criteria_enable, (1 << 0));
	DEVX_SET(create_flow_group_in, infg, match_criteria.outer_headers.dmac_47_16, __devx_mask(32));
	DEVX_SET(create_flow_group_in, infg, match_criteria.outer_headers.dmac_15_0, __devx_mask(16));

	root_flow_group = mlx5dv_devx_obj_create(vfcontext, infg, sizeof(infg), outfg, sizeof(outfg));
	if (!root_flow_group) {
		log_err("bad flow group %u %x", DEVX_GET(create_flow_group_out, outfg, status), DEVX_GET(create_flow_group_out, outfg, syndrome));
		return -1;
	}

	flow_group_number = DEVX_GET(create_flow_group_out, outfg, group_id);
	return 0;
}

static unsigned int ctx_max_doorbells(struct directpath_ctx *dp)
{
	return 4 * dp->nr_qs;
}

static size_t estimate_region_size(struct directpath_ctx *dp)
{
	uint32_t i;
	size_t total = 0;
	size_t wasted = 0;

	/* doorbells */
	total += CACHE_LINE_SIZE * ctx_max_doorbells(dp);

	for (i = 0; i < dp->nr_qs; i++) {
		wasted += align_up(total, PGSIZE_4KB) - total;
		total = align_up(total, PGSIZE_4KB);
		total += (1UL << DEFAULT_CQ_LOG_SZ) * sizeof(struct mlx5_cqe64);

		wasted += align_up(total, PGSIZE_4KB) - total;
		total = align_up(total, PGSIZE_4KB);
		total += (1UL << DEFAULT_CQ_LOG_SZ) * sizeof(struct mlx5_cqe64);

		wasted += align_up(total, PGSIZE_4KB) - total;
		total = align_up(total, PGSIZE_4KB);
		total += (1UL << DEFAULT_RQ_LOG_SZ) * MLX5_SEND_WQE_BB;

		wasted += align_up(total, PGSIZE_4KB) - total;
		total = align_up(total, PGSIZE_4KB);
		total += (1UL << DEFAULT_SQ_LOG_SZ) * MLX5_SEND_WQE_BB;
	}

	log_debug("reg size %lu, wasted %lu", total, wasted);

	return total;
}

static int alloc_from_uregion(struct directpath_ctx *dp, size_t size, size_t alignment, uint64_t *alloc_out)
{
	size_t aligned_start;

	if (!is_power_of_two(alignment))
		return -EINVAL;

	if (size == CACHE_LINE_SIZE) {

		if (dp->doorbells_allocated == dp->max_doorbells) {
			log_err("too many doorbells");
			return -ENOMEM;
		}

		*alloc_out = dp->doorbells_allocated++ * CACHE_LINE_SIZE;
		return 0;
	}

	aligned_start = align_up(dp->region_allocated, alignment);

	if (aligned_start > dp->region.len) {
		log_err("exhausted reg len");
		return -ENOMEM;
	}

	if (dp->region.len - aligned_start < size) {
		log_err("exhausted reg len");
		return -ENOMEM;
	}

	*alloc_out = aligned_start;
	dp->region_allocated = aligned_start + size;

	return 0;
}

static int activate_rq(struct qp *qp, uint32_t rqn)
{
	int ret;
	uint32_t in[DEVX_ST_SZ_DW(modify_rq_in)] = {0};
	uint32_t out[DEVX_ST_SZ_DW(modify_rq_out)] = {0};
	void *rqc;

	DEVX_SET(modify_rq_in, in, opcode, MLX5_CMD_OP_MODIFY_RQ);
	DEVX_SET(modify_rq_in, in, rq_state, 0 /* currently RST */);
	DEVX_SET(modify_rq_in, in, rqn, rqn);

	rqc = DEVX_ADDR_OF(modify_rq_in, in, ctx);
	DEVX_SET(rqc, rqc, state, 1 /* RDY */);

	ret = mlx5dv_devx_obj_modify(qp->rx_wq.obj, in, sizeof(in), out, sizeof(out));
	if (ret) {
		log_err("failed to set RQ to RDY");
		return ret;
	}

	return 0;
}

static int activate_sq(struct qp *qp)
{
	int ret;
	uint32_t in[DEVX_ST_SZ_DW(modify_sq_in)] = {0};
	uint32_t out[DEVX_ST_SZ_DW(modify_sq_out)] = {0};
	void *sqc;

	DEVX_SET(modify_sq_in, in, opcode, MLX5_CMD_OP_MODIFY_SQ);
	DEVX_SET(modify_sq_in, in, sq_state, 0 /* currently RST */);
	DEVX_SET(modify_sq_in, in, sqn, qp->sqn);

	sqc = DEVX_ADDR_OF(modify_sq_in, in, sq_context);
	DEVX_SET(sqc, sqc, state, 1 /* RDY */);

	ret = mlx5dv_devx_obj_modify(qp->tx_wq.obj, in, sizeof(in), out, sizeof(out));
	if (ret) {
		log_err("failed to set RQ to RDY");
		return ret;
	}

	return 0;
}

static int alloc_td(struct directpath_ctx *dp)
{
	uint32_t in[DEVX_ST_SZ_DW(alloc_transport_domain_in)] = {0};
	uint32_t out[DEVX_ST_SZ_DW(alloc_transport_domain_out)] = {0};

	DEVX_SET(alloc_transport_domain_in, in, opcode, MLX5_CMD_OP_ALLOC_TRANSPORT_DOMAIN);

	dp->td_obj = mlx5dv_devx_obj_create(vfcontext, in, sizeof(in), out, sizeof(out));
	if (!dp->td_obj) {
		log_err("couldn't create td obj");
		return -1;
	}

	dp->tdn = DEVX_GET(alloc_transport_domain_out, out, transport_domain);
	return 0;
}

static int create_tis(struct directpath_ctx *dp)
{
	uint32_t in[DEVX_ST_SZ_DW(create_tis_in)] = {0};
	uint32_t out[DEVX_ST_SZ_DW(create_tis_out)] = {0};
	void *ctx;

	DEVX_SET(create_tis_in, in, opcode, MLX5_CMD_OP_CREATE_TIS);
	ctx = DEVX_ADDR_OF(create_tis_in, in, ctx);

	DEVX_SET(tisc, ctx, tls_en, 0);
	DEVX_SET(tisc, ctx, transport_domain, dp->tdn);

	dp->tis_obj = mlx5dv_devx_obj_create(vfcontext, in, sizeof(in), out, sizeof(out));
	if (!dp->tis_obj) {
		log_err("failed to create TIS");
		return -1;
	}

	dp->tisn = DEVX_GET(create_tis_out, out, tisn);

	return 0;
}

static int fill_wqc(struct directpath_ctx *dp, struct qp *qp, struct wq *wq, void *wqc, uint32_t log_nr_wqe, bool is_rq)
{
	int ret;
	size_t wqe_cnt = 1UL << log_nr_wqe;
	uint64_t bufs, dbr;

	ret = alloc_from_uregion(dp, wqe_cnt * MLX5_SEND_WQE_BB, PGSIZE_4KB, &bufs);
	if (ret)
		return -ENOMEM;

	ret = alloc_from_uregion(dp, CACHE_LINE_SIZE, CACHE_LINE_SIZE, &dbr);
	if (ret)
		return -ENOMEM;

	DEVX_SET(wq, wqc, wq_type, 1 /* WQ_CYCLIC */); // TODO striding RQ
	DEVX_SET(wq, wqc, pd, dp->pdn);

	if (!is_rq)
		DEVX_SET(wq, wqc, uar_page, qp->uarn);

	DEVX_SET64(wq, wqc, dbr_addr, dbr);
	DEVX_SET(wq, wqc, log_wq_stride, MLX5_SEND_WQE_SHIFT);
	DEVX_SET(wq, wqc, log_wq_sz, log_nr_wqe);
	DEVX_SET(wq, wqc, dbr_umem_valid, 1);
	DEVX_SET(wq, wqc, wq_umem_valid, 1);
	DEVX_SET(wq, wqc, dbr_umem_id, dp->mem_reg->umem_id);
	DEVX_SET(wq, wqc, wq_umem_id, dp->mem_reg->umem_id);
	DEVX_SET64(wq, wqc, wq_umem_offset, bufs);

	wq->stride = 1UL << MLX5_SEND_WQE_SHIFT;
	wq->buf = dp->region.base + bufs;
	wq->dbrec = dp->region.base + dbr;
	wq->wqe_cnt = wqe_cnt;

	return 0;
}

static int create_sq(struct directpath_ctx *dp, struct qp *qp, uint32_t log_nr_wqe)
{
	int ret;
	uint32_t in[DEVX_ST_SZ_DW(create_sq_in)] = {0};
	uint32_t out[DEVX_ST_SZ_DW(create_sq_out)] = {0};
	void *sqc, *wqc;

	DEVX_SET(create_sq_in, in, opcode, MLX5_CMD_OP_CREATE_SQ);
	sqc = DEVX_ADDR_OF(create_sq_in, in, ctx);

	DEVX_SET(sqc, sqc, cqn, qp->tx_cq.cqn);
	DEVX_SET(sqc, sqc, tis_lst_sz, 1);
	DEVX_SET(sqc, sqc, tis_num_0, dp->tisn);

	wqc = DEVX_ADDR_OF(sqc, sqc, wq);
	ret = fill_wqc(dp, qp, &qp->tx_wq, wqc, log_nr_wqe, false);
	if (ret)
		return ret;

	qp->tx_wq.obj = mlx5dv_devx_obj_create(vfcontext, in, sizeof(in), out, sizeof(out));
	if (!qp->tx_wq.obj) {
		log_err("failed to create sq obj %d %x", DEVX_GET(create_sq_out, out, status), DEVX_GET(create_sq_out, out, syndrome));
		return -1;
	}

	BUILD_ASSERT(MLX5_SEND_WQE_BB == 1 << MLX5_SEND_WQE_SHIFT);

	qp->sqn = DEVX_GET(create_sq_out, out, sqn);

	return activate_sq(qp);
}


static int create_rq(struct directpath_ctx *dp, struct qp *qp, uint32_t log_nr_wqe, int idx)
{
	int ret;
	uint32_t in[DEVX_ST_SZ_DW(create_rq_in)] = {0};
	uint32_t out[DEVX_ST_SZ_DW(create_rq_out)] = {0};
	void *rq_ctx, *wq_ctx;

	DEVX_SET(create_rq_in, in, opcode, MLX5_CMD_OP_CREATE_RQ);
	rq_ctx = DEVX_ADDR_OF(create_rq_in, in, ctx);

	DEVX_SET(rqc, rq_ctx, delay_drop_en, 0); // TODO: can this work?
	DEVX_SET(rqc, rq_ctx, mem_rq_type, 0 /* RQ_INLINE */);
	DEVX_SET(rqc, rq_ctx, state, 0 /* RST */);

	DEVX_SET(rqc, rq_ctx, cqn, qp->rx_cq.cqn);

	wq_ctx = DEVX_ADDR_OF(rqc, rq_ctx, wq);

	ret = fill_wqc(dp, qp, &qp->rx_wq, wq_ctx, log_nr_wqe, true);
	if (ret)
		return ret;

	qp->rx_wq.obj = mlx5dv_devx_obj_create(vfcontext, in, sizeof(in), out, sizeof(out));
	if (!qp->rx_wq.obj) {
		log_err("create rq obj failed status %d syndrome %x", DEVX_GET(create_rq_out, out, status), DEVX_GET(create_rq_out, out, syndrome));
		return -1;
	}

	dp->rqns[idx] = DEVX_GET(create_rq_out, out, rqn);
	return activate_rq(qp, dp->rqns[idx]);
}

static int create_cq(struct directpath_ctx *dp, struct cq *cq, uint32_t log_nr_cq, bool monitored, uint32_t qp_idx)
{
	int i, ret;
	uint32_t in[DEVX_ST_SZ_DW(create_cq_in)] = {0};
	uint32_t out[DEVX_ST_SZ_DW(create_cq_out)] = {0};
	uint64_t cqe_cnt = 1U << log_nr_cq;
	void *cq_ctx;
	uint64_t bufs, dbr;

	DEVX_SET(create_cq_in, in, opcode, MLX5_CMD_OP_CREATE_CQ);
	cq_ctx = DEVX_ADDR_OF(create_cq_in, in, cq_context);

	ret = alloc_from_uregion(dp, cqe_cnt * sizeof(struct mlx5_cqe64), PGSIZE_4KB, &bufs);
	if (ret)
		return -ENOMEM;

	ret = alloc_from_uregion(dp, CACHE_LINE_SIZE, CACHE_LINE_SIZE, &dbr);
	if (ret)
		return -ENOMEM;

	cq->buf = dp->region.base + bufs;
	cq->dbrec = dp->region.base + dbr;
	cq->cqe_cnt = cqe_cnt;

	for (i = 0; i < cqe_cnt; i++)
		mlx5dv_set_cqe_owner(&cq->buf[i], 1);

	DEVX_SET(cqc, cq_ctx, cqe_sz, 0 /* 64B CQE */); // TODO 1 for INDIRECT
	DEVX_SET(cqc, cq_ctx, cc, 0 /* not collapsed to first entry */); // TODO 1 for INDIRECT
	DEVX_SET(cqc, cq_ctx, scqe_break_moderation_en, 0); // solicited CQE does not break event moderation!
	DEVX_SET(cqc, cq_ctx, oi, 0 /* no overrun ignore */);

	enum {
		CQ_PERIOD_MODE_UPON_EVENT = 0,
		CQ_PERIOD_MODE_UPON_CQE = 1,
	};

	DEVX_SET(cqc, cq_ctx, cq_period_mode, 1); // TODO figure this out
	DEVX_SET(cqc, cq_ctx, cqe_comp_en, 0 /* no compression */); // TODO enable this
	// DEVX_SET(cqc, cq_ctx, mini_cqe_res_format, );
	// DEVX_SET(cqc, cq_ctx, cqe_comp_layout, 0 /* BASIC_CQE_COMPRESSION */);
	// DEVX_SET(cqc, cq_ctx, mini_cqe_res_format_ext, );
	DEVX_SET(cqc, cq_ctx, cq_timestamp_format, 0 /* INTERNAL_TIMER */);
	DEVX_SET(cqc, cq_ctx, log_cq_size, log_nr_cq);
	// TODO maybe set this to something else for non-monitored
	DEVX_SET(cqc, cq_ctx, uar_page, main_eq.uar->page_id); // TODO FIX

	if (monitored) {
		DEVX_SET(cqc, cq_ctx, cq_period, 0);
		DEVX_SET(cqc, cq_ctx, cq_max_count, 1); // TODO?
	} else {
		DEVX_SET(cqc, cq_ctx, cq_period, 0);
		DEVX_SET(cqc, cq_ctx, cq_max_count, 0);
	}

	DEVX_SET(cqc, cq_ctx, c_eqn, main_eq.eqn);

	DEVX_SET(create_cq_in, in, cq_umem_valid, 1);
	DEVX_SET(create_cq_in, in, cq_umem_id, dp->mem_reg->umem_id);
	DEVX_SET64(create_cq_in, in, cq_umem_offset, bufs);

	DEVX_SET(cqc, cq_ctx, dbr_umem_valid, 1);
	DEVX_SET(cqc, cq_ctx, dbr_umem_id, dp->mem_reg->umem_id);
	DEVX_SET64(cqc, cq_ctx, dbr_addr, dbr);

	cq->obj = mlx5dv_devx_obj_create(vfcontext, in, sizeof(in), out, sizeof(out));
	if (!cq->obj) {
		log_err("create cq obj failed status %d syndrome %x", DEVX_GET(create_cq_out, out, status), DEVX_GET(create_cq_out, out, syndrome));
		return -1;
	}

	cq->cqn = DEVX_GET(create_cq_out, out, cqn);

	if (monitored) {
		cq->qp_idx = qp_idx;
		BUG_ON(cq->cqn >= MAX_CQ);
		cqn_to_cq_map[cq->cqn] = cq;
		if (cfg.no_directpath_active_rss || qp_idx == 0) {
			dp->active_rx_count++;
			bitmap_set(dp->active_rx_queues, qp_idx);
			cq->state = RXQ_STATE_ACTIVE;
		} else {
			dp->disabled_rx_count++;
			cq->state = RXQ_STATE_DISABLED;
		}
	}

	return 0;
}

bool directpath_poll(void)
{
	bool work_done;

	work_done = directpath_events_poll();

	work_done |= directpath_commands_poll();

	return work_done;
}

static int create_rqt(struct directpath_ctx *dp)
{
	size_t inlen;
	uint32_t *in, i;
	uint32_t out[DEVX_ST_SZ_DW(create_rqt_out)] = {0};
	uint32_t nr_entries = u32_round_pow2(dp->nr_qs);
	void *rqtc;

	inlen = DEVX_ST_SZ_BYTES(create_rqt_in) + DEVX_ST_SZ_BYTES(rq_num) * nr_entries;
	in = calloc(1, inlen);
	if (!in)
		return -ENOMEM;

	DEVX_SET(create_rqt_in, in, opcode, MLX5_CMD_OP_CREATE_RQT);
	rqtc = DEVX_ADDR_OF(create_rqt_in, in, rqt_context);

	DEVX_SET(rqtc, rqtc, rqt_max_size, nr_entries);
	DEVX_SET(rqtc, rqtc, rqt_actual_size, nr_entries);

	for (i = 0; i < nr_entries; i++) {
		unsigned int idx = cfg.no_directpath_active_rss ? (i % dp->nr_qs) : 0;
		DEVX_SET(rqtc, rqtc, rq_num[i], dp->rqns[idx]);
	}

	dp->rqt_obj = mlx5dv_devx_obj_create(vfcontext, in, inlen, out, sizeof(out));
	free(in);
	if (!dp->rqt_obj) {
		log_err("create rqt obj failed");
		return -1;
	}

	dp->rqtn = DEVX_GET(create_rqt_out, out, rqtn);

	return 0;
}

static int create_tir(struct directpath_ctx *dp)
{
	uint32_t in[DEVX_ST_SZ_DW(create_tir_in)] = {0};
	uint32_t out[DEVX_ST_SZ_DW(create_tir_out)] = {0};
	void *tir_ctx, *hf;

	DEVX_SET(create_tir_in, in, opcode, MLX5_CMD_OP_CREATE_TIR);
	tir_ctx = DEVX_ADDR_OF(create_tir_in, in, ctx);

	DEVX_SET(tirc, tir_ctx, disp_type, 1 /* INDIRECT */);
	DEVX_SET(tirc, tir_ctx, lro_enable_mask, 0);

	DEVX_SET(tirc, tir_ctx, rx_hash_symmetric, 0);
	DEVX_SET(tirc, tir_ctx, indirect_table, dp->rqtn);
	DEVX_SET(tirc, tir_ctx, rx_hash_fn, 2 /* TOEPLITZ */);
	DEVX_SET(tirc, tir_ctx, self_lb_block, 0); // TODO?

	DEVX_SET(tirc, tir_ctx, transport_domain, dp->tdn);

	BUILD_ASSERT(DEVX_FLD_SZ_BYTES(tirc, rx_hash_toeplitz_key) == sizeof(rss_key));
	memcpy(DEVX_ADDR_OF(tirc, tir_ctx, rx_hash_toeplitz_key), rss_key, sizeof(rss_key));

	hf = DEVX_ADDR_OF(tirc, tir_ctx, rx_hash_field_selector_outer);
	DEVX_SET(rx_hash_field_select, hf, l3_prot_type, 0 /* IPV4 */);
	DEVX_SET(rx_hash_field_select, hf, l4_prot_type, 1 /* UDP */); //TODO: TCP TIR also
	DEVX_SET(rx_hash_field_select, hf, selected_fields, 0xF /* L3 + L4, IP/PORT */);

	dp->tir_obj = mlx5dv_devx_obj_create(vfcontext, in, sizeof(in), out, sizeof(out));
	if (!dp->tir_obj) {
		log_err("create tis obj failed");
		return -1;
	}

	dp->tirn = DEVX_GET(create_tir_out, out, tirn);

	return 0;
}

static ssize_t alloc_uar(void)
{
	uint32_t out[DEVX_ST_SZ_DW(alloc_uar_out)] = {};
	uint32_t in[DEVX_ST_SZ_DW(alloc_uar_in)] = {};
	int err;

	DEVX_SET(alloc_uar_in, in, opcode, MLX5_CMD_OP_ALLOC_UAR);
	err = mlx5dv_devx_general_cmd(vfcontext, in, sizeof(in), out, sizeof(out));
	if (err) {
		log_err("failed to alloc uar");
		return -1;
	}

	return DEVX_GET(alloc_uar_out, out, uar);
}

static void dealloc_uar(ssize_t uarn)
{
	uint32_t out[DEVX_ST_SZ_DW(dealloc_uar_out)] = {};
	uint32_t in[DEVX_ST_SZ_DW(dealloc_uar_in)] = {};
	int err;

	DEVX_SET(dealloc_uar_in, in, opcode, MLX5_CMD_OP_DEALLOC_UAR);
	DEVX_SET(dealloc_uar_in, in, uar, uarn);

	err = mlx5dv_devx_general_cmd(vfcontext, in, sizeof(in), out, sizeof(out));
	if (err)
		log_err("failed to free uarn %ld", uarn);
}

static int allocate_user_memory(struct directpath_ctx *dp)
{
	int ret;

	dp->memfd = memfd_create("directpath", 0); // Maybe huge?
	if (dp->memfd < 0) {
		log_err("failed to create memfd!");
		return -errno;
	}

	dp->region.len = estimate_region_size(dp);
	ret = ftruncate(dp->memfd, dp->region.len);
	if (ret) {
		log_err("failed to ftruncate");
		return -errno;
	}

	dp->region.base = mmap(NULL, dp->region.len, PROT_READ | PROT_WRITE,
	                       MAP_SHARED, dp->memfd, 0);
	if (dp->region.base == MAP_FAILED) {
		log_err("bad mmap");
		return -errno;
	}

	// probably unnecessary
	memset(dp->region.base, 0, dp->region.len);

	dp->max_doorbells = ctx_max_doorbells(dp);
	dp->doorbells_allocated = 0;
	dp->region_allocated = dp->max_doorbells * CACHE_LINE_SIZE;

	dp->mem_reg = mlx5dv_devx_umem_reg(vfcontext, dp->region.base,
	                                   dp->region.len, IBV_ACCESS_LOCAL_WRITE);
	if (!dp->mem_reg) {
		log_err("register mem failed");
		return -1;
	}

	return 0;
}

static void free_qp(struct qp *qp)
{
	int ret;

	if (qp->rx_wq.obj) {
		ret = mlx5dv_devx_obj_destroy(qp->rx_wq.obj);
		if (ret)
			log_warn("failed to destroy rx wq obj");
	}

	if (qp->rx_cq.obj) {
		ret = mlx5dv_devx_obj_destroy(qp->rx_cq.obj);
		if (ret)
			log_warn("failed to destroy rx cq obj");
	}

	if (qp->tx_wq.obj) {
		ret = mlx5dv_devx_obj_destroy(qp->tx_wq.obj);
		if (ret)
			log_warn("failed to destroy tx wq obj");
	}

	if (qp->tx_cq.obj) {
		ret = mlx5dv_devx_obj_destroy(qp->tx_cq.obj);
		if (ret)
			log_warn("failed to destroy tx cq obj");
	}

}

void free_ctx(struct proc *p)
{
	int i, ret;
	struct directpath_ctx *ctx = (struct directpath_ctx *)p->directpath_data;

	if (!ctx)
		return;

	if (ctx->mreg) {
		ret = ibv_dereg_mr(ctx->mreg);
		if (ret)
			log_warn("couldn't free MR");
	}

	if (ctx->fte) {
		ret = mlx5dv_devx_obj_destroy(ctx->fte);
		if (ret) {
			log_warn("couldn't destroy STE obj");
		}

		bitmap_clear(mac_used_entries, ctx->flow_tbl_index);
	}

	if (ctx->tir_obj) {
		ret = mlx5dv_devx_obj_destroy(ctx->tir_obj);
		if (ret)
			log_warn("failed to destroy tir obj");
	}

	if (ctx->rqt_obj) {
		ret = mlx5dv_devx_obj_destroy(ctx->rqt_obj);
		if (ret)
			log_warn("failed to destroy rqt obj");
	}

	for (i = 0; i < ctx->nr_qs; i++)
		free_qp(&ctx->qps[i]);

	if (ctx->tis_obj) {
		ret = mlx5dv_devx_obj_destroy(ctx->tis_obj);
		if (ret)
			log_warn("failed to destroy td obj");
	}


	if (ctx->td_obj) {
		ret = mlx5dv_devx_obj_destroy(ctx->td_obj);
		if (ret)
			log_warn("failed to destroy td obj");
	}

	if (ctx->mem_reg) {
		if (mlx5dv_devx_umem_dereg(ctx->mem_reg))
			log_warn("failed to unreg mem");
	}

	if (ctx->region.base != MAP_FAILED) {
		if (munmap(ctx->region.base, ctx->region.len))
			log_warn("failed to munmap region: %d", errno);
	}

	if (ctx->memfd >= 0)
		close(ctx->memfd);

	if (ctx->uarns) {
		for (i = 0; i < ctx->nr_alloc_uarn; i++)
			dealloc_uar(ctx->uarns[i]);
		free(ctx->uarns);
	}

	if (ctx->pd)
		ibv_dealloc_pd(ctx->pd);

	free(ctx->rqns);
	free(ctx);
}

int directpath_get_clock(unsigned int *frequency_khz, void **core_clock)
{
	return mlx5_vfio_get_clock(vfcontext, frequency_khz, core_clock);
}

int alloc_directpath_ctx(struct proc *p, struct directpath_spec *spec_out,
                         int *memfd_out, int *barfd_out)
{
	int ret = 0;
	uint32_t i;
	struct directpath_ctx *dp;
	struct mlx5dv_pd pd_out;
	struct mlx5dv_obj init_obj;

	BUG_ON(!vfcontext);

	dp = malloc(sizeof(*dp) + p->thread_count * sizeof(struct qp));
	if (!dp)
		return -ENOMEM;

	memset(dp, 0, sizeof(*dp) + p->thread_count * sizeof(struct qp));
	dp->p = p;
	p->directpath_data = (unsigned long)dp;
	dp->memfd = -1;
	dp->command_slot = COMMAND_SLOT_UNALLOCATED;
	dp->region.base = MAP_FAILED;
	dp->nr_qs = p->thread_count;
	dp->rqns = malloc(sizeof(*dp->rqns) * p->thread_count);
	if (unlikely(!dp->rqns))
		goto err;

	dp->pd = ibv_alloc_pd(vfcontext);
	if (!dp->pd)
		goto err;

	init_obj.pd.in = dp->pd;
	init_obj.pd.out = &pd_out;
	ret = mlx5dv_init_obj(&init_obj, MLX5DV_OBJ_PD);
	if (ret) {
		log_err("failed to get pdn");
		goto err;
	}
	dp->pdn = pd_out.pdn;

	ret = allocate_user_memory(dp);
	if (ret)
		goto err;

	ret = alloc_td(dp);
	if (ret)
		goto err;

	ret = create_tis(dp);
	if (ret)
		goto err;

	dp->uarns = malloc(sizeof(*dp->uarns) * dp->nr_qs);
	if (!dp->uarns)
		goto err;


	/* For now allocate 1 UAR, may need more for scalability reasons ? */
	for (i = 0; i < 1; i++) {
		dp->uarns[i] = alloc_uar();
		if (dp->uarns[i] == -1)
			goto err;
		dp->nr_alloc_uarn++;
	}

	for (i = 0; i < dp->nr_qs; i++) {

		/* round robin assign every pair of SQs to a UARN */
		dp->qps[i].uarn = dp->uarns[(i / 2) % dp->nr_alloc_uarn];
		dp->qps[i].uar_offset = i % 2 == 0 ? 0x800 : 0xA00;

		ret = create_cq(dp, &dp->qps[i].rx_cq, DEFAULT_CQ_LOG_SZ, true, i);
		if (ret)
			goto err;

		ret = create_cq(dp, &dp->qps[i].tx_cq, DEFAULT_CQ_LOG_SZ, false, i);
		if (ret)
			goto err;

		ret = create_rq(dp, &dp->qps[i], DEFAULT_RQ_LOG_SZ, i);
		if (ret)
			goto err;

		ret = create_sq(dp, &dp->qps[i], DEFAULT_SQ_LOG_SZ);
		if (ret)
			goto err;

		/* fill spec for runtime */
		qp_fill_spec(&dp->region, &dp->qps[i], &spec_out->qs[i]);

		/* fill info for iokernel polling */
		qp_fill_iokspec(&p->threads[i], &dp->qps[i]);
	}

	ret = create_rqt(dp);
	if (ret)
		goto err;

	ret = create_tir(dp);
	if (ret)
		goto err;

	dp->mreg = ibv_reg_mr(dp->pd, p->region.base, p->region.len,
	                      IBV_ACCESS_LOCAL_WRITE);
	if (!dp->mreg) {
		log_err("failed to create mr");
		goto err;
	}

	ret = directpath_activate_rx(dp);
	if (ret)
		goto err;

	*memfd_out = dp->memfd;
	*barfd_out = bar_fd;

	spec_out->memfd_region_size = dp->region.len;
	spec_out->mr = dp->mreg->lkey;
	spec_out->va_base = (uintptr_t)p->region.base;
	spec_out->offs = bar_offs;
	spec_out->bar_map_size = bar_map_size;

	return 0;

err:
	log_err("err in allocating process (%d)", ret);
	free_ctx(p);
	return ret ? ret : -1;
}

int directpath_init(void)
{
	int ret;
	struct mlx5dv_vfio_context_attr vfattr;
	struct ibv_device **dev_list;

	if (!cfg.vfio_directpath)
		return 0;

	if (!nic_pci_addr_str) {
		log_err("please supply the pci address for the nic");
		return -EINVAL;
	}

	memset(&vfattr, 0, sizeof(vfattr));
	vfattr.pci_name = nic_pci_addr_str;
	dev_list = mlx5dv_get_vfio_device_list(&vfattr);
	if (!dev_list) {
		log_err("could not find matching vfio device for pci address %s", nic_pci_addr_str);
		return -1;
	}

	vfcontext = ibv_open_device(dev_list[0]);
	BUG_ON(!vfcontext);

	ret = directpath_commands_init();
	if (ret)
		return ret;

	ret = setup_steering();
	if (ret) {
		log_err("failed to init steering %d", ret);
		return ret;
	}

	/* after this point, events are routed to the dataplane eq */
	ret = events_init();
	if (ret)
		return ret;

	export_fd(vfcontext, &bar_fd, &bar_offs, &bar_map_size);

	return 0;
}

#else

int directpath_init(void)
{
	return 0;
}

#endif
