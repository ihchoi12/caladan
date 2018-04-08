extern "C" {
#include <base/log.h>
}

#include "thread.h"
#include "sync.h"
#include "timer.h"
#include "fake_worker.h"

#include <iostream>

namespace {

int threads;
uint64_t n;
std::string worker_spec;

void MainHandler(void *arg) {
  rt::WaitGroup wg(1);
  uint64_t cnt[threads] = {};

  for (int i = 0; i < threads; ++i) {
    rt::Spawn([&,i](){
      auto *w = FakeWorkerFactory(worker_spec);
      if (w == nullptr) {
        std::cerr << "Failed to create worker." << std::endl;
        exit(1);
      }

      while (true) {
        w->Work(n);
        cnt[i]++;
        rt::Yield();
      }
    });
  }

  rt::Spawn([&](){
    uint64_t last_total = 0;
    while (1) {
      uint64_t total = 0;
      for (int i = 0; i < threads; i++) total += cnt[i];
      log_info("%ld", total - last_total);
      last_total = total;
      rt::Sleep(rt::kSeconds);
    }
  });

  // never returns
  wg.Wait();
}

} // anonymous namespace

int main(int argc, char *argv[]) {
  int ret;

  if (argc != 5) {
    std::cerr << "usage: [config_file] [#threads] [#n] [worker_spec]"
              << std::endl;
    return -EINVAL;
  }

  threads = std::stoi(argv[2], nullptr, 0);
  n = std::stoul(argv[3], nullptr, 0);
  worker_spec = std::string(argv[4]);

  ret = runtime_init(argv[1], MainHandler, NULL);
  if (ret) {
    printf("failed to start runtime\n");
    return ret;
  }

  return 0;
}
