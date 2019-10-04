#ifndef KVTEST1_HH
#define KVTEST1_HH
#include <byteswap.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/resource.h>

typedef unsigned char u8;
typedef unsigned int u32;
typedef unsigned long u64;

  static u64
xorshift(const u64 seed)
{
  u64 x = seed ? seed : 88172645463325252lu;
  x ^= x >> 12; // a
  x ^= x << 25; // b
  x ^= x >> 27; // c
  return x * UINT64_C(2685821657736338717);
}

static __thread u64 __random_seed_u64 = 0;

  static u64
random_u64(void)
{
  __random_seed_u64 = xorshift(__random_seed_u64);
  return __random_seed_u64;
}

struct kv {
  u32 klen;
  u32 vlen;
  u64 hash; // hashvalue of the key
  u8 kv[];  // len(kv) == klen + vlen
} __attribute__((packed));

static u64 __nr_samples;
static struct kv ** __samples;

template <typename C>
  static bool
load_mmapkv(C &client, const char * const fn)
{
  const int fd = open(fn, O_RDONLY);
  if (fd < 0) return false;
  const off_t filesz = lseek(fd, 0, SEEK_END);
  if (filesz < 0) return false;
  lseek(fd, 0, SEEK_SET);
  const u64 msize = ((((u64)filesz + 65536lu) >> 30) + 1lu) << 30;
  u8 * const m = (u8 *)malloc(msize);
  if (m == NULL) return false;
  u64 todo = (u64)filesz;
  u64 off = 0;
  while (todo) {
    const u64 nread = todo < (1lu << 20) ? todo : (1lu << 20);
    ssize_t n = read(fd, m + off, nread);
    if (n <= 0) {
      return false;
    }
    off += n;
    todo -= n;
  }
  close(fd);
  u64 * const offs = (typeof(offs))m;
  const u64 nr_kvs = (*offs) / sizeof(void *);
  __nr_samples = nr_kvs;
  __samples = (typeof(__samples))m;
  u64 inc=(u64)m;
  for (u64 i = 0; i < nr_kvs; i++) {
    offs[i] += inc;
  }
  fprintf(stderr, "%s filesz %lu nr %lu\n", __func__, (u64)filesz, nr_kvs);
  return true;
}

template <typename C>
  static bool
load_genkv(C &client, const char * const fn)
{
  FILE * const fin = fopen(fn, "r");
  char * buf = NULL;
  char * buf2 = NULL;
  size_t bufsize = 0;
  // line 1: nr_keys klen
  if ((getline(&buf, &bufsize, fin) <= 0) || ((buf2 = strrchr(buf, ' ')) == NULL)) return false;
  const u64 nr = strtoull(buf, NULL, 10);
  const u64 klen = strtoull(buf2 + 1, NULL, 10);
  // get full size
  u64 * const ranges = new u64[klen];
  for (u64 i = 0; i < klen; i++) ranges[i] = 1;
  // line 2+: key-offset nr-tokens (in [1,255])
  while ((getline(&buf, &bufsize, fin) > 0) && (buf2 = strrchr(buf, ' '))) {
    const u64 koff = strtoull(buf, NULL, 10);
    const u64 nt = strtoull(buf2 + 1, NULL, 10);
    if (koff < klen) {
      ranges[koff] = (nt > 255) ? 255 : nt;
    } else {
      fprintf(stderr, "koff out of range (%lu < %lu)\n", koff, klen);
    }
  }
  fclose(fin);
  u8 * keybuf = new u8[65536];
  u8 * valbuf = new u8[65536];
  __nr_samples = nr;
  __samples = new struct kv *[nr];
  for (u64 i = 0; i < nr; i++) {
    u64 v = i;
    memset(keybuf, 1, klen);
    for (u64 k = klen; k; k--) {
      const u64 r = ranges[k-1];
      if (r > 1) {
        keybuf[k-1] = (v % r) + 1;
        v /= r;
      }
    }
    struct kv * const kv = reinterpret_cast<struct kv *> (new char[sizeof(struct kv) + klen]);
    memcpy(kv->kv, keybuf, klen);
    kv->klen = klen;
    kv->vlen = 0;
    kv->hash = 0;
    __samples[i] = kv;
  }
  return true;
}

template <typename C>
  static bool
load_any(C &client, const char * const fn)
{
  if (strstr(fn, ".genkv")) {
    return load_genkv(client, fn);
  } else if (strstr(fn, ".mmapkv")) {
    return load_mmapkv(client, fn);
  }
  return false;
}

  template <typename C>
void kvtest_fill1(C &client, u64 nr)
{
  static bool filldone = false;// sync between clients
  if (client.id() == 0) {
    {
      struct rusage rs;
      getrusage(RUSAGE_SELF, &rs);
      const u64 kb0 = rs.ru_maxrss;
      // fill now
      double t0 = client.now();
      for (u64 i = 0; i < nr; i++) {
        if ((i + 1) < nr) {
          __builtin_prefetch(__samples[i+1], 0, 1);
        }
        struct kv * const kv = __samples[i];
        client.put(Str(kv->kv, kv->klen), Str(kv->kv + kv->klen, kv->vlen));
      }
      double dt = client.now() - t0;
      getrusage(RUSAGE_SELF, &rs);
      const u64 kb1 = rs.ru_maxrss;
      const u64 dkb = kb1 - kb0;
      fprintf(stderr, "rss_kb +%lu rss_mb +%lu\n", dkb, dkb>>10);
      const double mops = (double)nr / (dt * 1000000.0);
      fprintf(stderr, "rgen incu pass 1 0 0 0 0 4 100 0 0 0 set %lu %lu mops %.4lf\n", nr, nr, mops);
      fprintf(stderr, "rgen incu pass 1 0 0 0 0 2 s 0 set %lu %lu mops %.4lf\n", nr, nr, mops);
    }
    {
      Str valout;
      double t0 = client.now();
      for (u64 i = 0; i < nr; i++) {
        struct kv * const kv = __samples[i];
        client.get_sync(Str(kv->kv, kv->klen), valout);
      }
      double dt = client.now() - t0;
      const double mops = (double)nr / (dt * 1000000.0);
      fprintf(stderr, "rgen incu pass 1 0 0 0 0 4 0 0 0 0 pro %lu %lu mops %.4lf\n", nr, nr, mops);
      fprintf(stderr, "rgen incu pass 1 0 0 0 0 2 p 0 pro %lu %lu mops %.4lf\n", nr, nr, mops);
    }

    filldone = true;
  } else {
    while (filldone == false) {
      usleep(1);
    }
  }
}

  template <typename C>
void kvtest_del1(C &client, u64 nr)
{
  if (client.id() == 0) {
    sleep(3); // wait long enough for readers to finish
    double t0 = client.now();
    for (u64 i = 0; i < nr; i++) {
      if ((i + 1) < nr) {
        __builtin_prefetch(__samples[i+1], 0, 1);
      }
      struct kv * const kv = __samples[i];
      client.remove(Str(kv->kv, kv->klen));
    }
    double dt = client.now() - t0;
    const double mops = (double)nr / (dt * 1000000.0);
    fprintf(stderr, "rgen incu pass 1 0 0 0 0 4 0 100 0 0 del %lu %lu mops %.4lf\n", nr, nr, mops);
    fprintf(stderr, "rgen incu pass 1 0 0 0 0 2 d 0 del %lu %lu mops %.4lf\n", nr, nr, mops);
  }
}

  template <typename C>
void kvtest_body1(C &client, u64 nr, u64 pset)
{
  Str valout;
  double t0 = client.now();
  do {
    for (int i = 0; i < 4096; i++) {
      const u64 r = random_u64();
      struct kv * const kv = __samples[r % nr];
      client.get_sync(Str(kv->kv, kv->klen), valout);
    }
  } while (client.now() - t0 < 10.0);

  u64 count = 0;
  u64 set = 0;
  u64 get = 0;
  u64 hit = 0;
  std::vector<Str> keys, values;

  t0 = client.now();
  u64 idp = random_u64() % nr;
  struct kv * kvp = __samples[idp];
  __builtin_prefetch(kvp, 0, 1);

  idp = random_u64() % nr;
  __builtin_prefetch(&(__samples[idp]), 0, 1);

  do {
    for (int i = 0; i < 1000; i++) {
      struct kv * const kv = kvp;
      kvp = __samples[idp];
      __builtin_prefetch(kvp, 0, 1);
      if (i < 998) {
        idp = random_u64() % nr;
        __builtin_prefetch(&(__samples[idp]), 0, 1);
      }
      if ((random_u64() % 100) < pset) {
        set++;
        client.put(Str(kv->kv, kv->klen), Str(kv->kv + kv->klen, kv->vlen));
      } else {
        get++;
        if (client.get_sync(Str(kv->kv, kv->klen), valout)) hit++;
        //client.scan_sync(Str(kv->kv, kv->klen), 100, keys, values); hit++;
      }
    }
  } while (client.now() - t0 < 10.0);
  double dt = client.now() - t0;

  if (client.id() == 0) {
    set *= client.nthreads();
    get *= client.nthreads();
    hit *= client.nthreads();
    count = set + get;
    const double mops = (double)count / (dt * 1000000.0);
    fprintf(stderr, "rgen uniform pass %d 0 0 0 0 4 %d 0 0 0 set %lu %lu pro %lu %lu mops %.4lf\n",
        client.nthreads(), pset, set, set, get, hit, mops);
    if (pset == 0)
      fprintf(stderr, "rgen uniform pass %d 0 0 0 0 2 p 0 set 0 0 pro %lu %lu mops %.4lf\n",
          client.nthreads(), get, hit, mops);
  }
}

  template <typename C>
void kvtest_xkv(C &client)
{
  // todo get env input file
  const int nr = atoi(getenv("MTT_NR"));
  const int pset = atoi(getenv("MTT_PSET"));
  char * const fn = strdup(getenv("MTT_FILE"));

  if (client.id() == 0) {
    if (load_any(client, fn) == false) {
      client.notice("fail loading\n");
      exit(0);
    }
  }

  kvtest_fill1(client, nr);
  kvtest_body1(client, nr, pset);
  kvtest_del1(client, nr);
}
#endif
