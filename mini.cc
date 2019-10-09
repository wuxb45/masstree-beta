#include "query_masstree.hh"
#include "masstree_tcursor.hh"
#include "masstree_insert.hh"
#include "masstree_remove.hh"
#include "masstree_scan.hh"

volatile mrcu_epoch_type globalepoch = 1;
volatile mrcu_epoch_type active_epoch = 1;
kvepoch_t global_log_epoch = 0;
kvtimestamp_t initial_timestamp;

struct mtref {
  Masstree::default_table * table_;
  threadinfo * ti_;
  query<row_type> q_;
  void rcuq(void) {
    mrcu_epoch_type e = timestamp() >> 16;
    if (e != globalepoch) {
      //workers.lock();
      if (mrcu_signed_epoch_type(e - globalepoch) > 0) {
        globalepoch = e;
        active_epoch = threadinfo::min_active_epoch();
      }
      //workers.unlock();
    }
    ti_->rcu_quiesce();
  }
  void put(const Str &key, const Str &value) {
    q_.run_replace(table_->table(), key, value, *ti_);
    rcuq();
  }
  bool get(const Str &key, Str &value) {
    bool r = q_.run_get1(table_->table(), key, 0, value, *ti_);
    rcuq();
    return r;
  }
  bool del(const Str &key) {
    bool r = q_.run_remove(table_->table(), key, *ti_);
    rcuq();
    return r;
  }
};


  int
main(int argc, char ** argv)
{
  threadinfo *main_ti = threadinfo::make(threadinfo::TI_MAIN, -1);
  main_ti->pthread() = pthread_self();
  Masstree::default_table * table = new Masstree::default_table;
  table->initialize(*main_ti);

  struct mtref ref;
  threadinfo *worker_ti = threadinfo::make(threadinfo::TI_PROCESS, 0);
  ref.ti_ = worker_ti;
  ref.table_ = table;

  char * keybuf = new char[24];
  char * valbuf = new char[24];
  printf("keybuf %p valbuf %p\n", keybuf, valbuf);
#define TESTNR ((1000))
  for (int x = 0; x < 1000000000; x += TESTNR) {
    for (int i = 0; i < TESTNR; i++) {
      sprintf(keybuf, "%20d", x+i);
      sprintf(valbuf, "%10d%10d", i,-i);
      ref.put(Str(keybuf, 20), Str(valbuf, 20));
    }
    for (int i = 0; i < TESTNR; i++) {
      sprintf(keybuf, "%20d", x+i);
      Str s;
      ref.get(Str(keybuf, 20), s);
      //printf("get %p %d %.*s\n", s.data(), s.length(), s.length(), s.data());
    }
    for (int i = 0; i < TESTNR; i++) {
      sprintf(keybuf, "%20d", x+i);
      ref.del(Str(keybuf, 20));
    }
  }
  return 0;
}
