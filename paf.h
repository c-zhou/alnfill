#ifndef PAF_PAF_H
#define PAF_PAF_H

#include <stdint.h>
#include <sys/types.h>

#include "misc.h"

#ifndef KSTRING_T
#define KSTRING_T kstring_t
typedef struct __kstring_t {
	size_t l, m;
	char *s;
} kstring_t;
#endif

typedef struct {
	void *fp;
	kstring_t buf;
} paf_file_t;

typedef struct {
	const char *qn, *tn, *aux; // these point to the input string; NOT allocated
	uint32 ql, qs, qe, tl, ts, te;
	uint32 ml:31, rev:1, bl, mq;
} paf_rec_t;

#ifdef __cplusplus
extern "C" {
#endif

paf_file_t *paf_open(const char *fn);
int paf_close(paf_file_t *pf);
int paf_read(paf_file_t *pf, paf_rec_t *r);
char *paf_read_line(paf_file_t *pf);
int paf_recover_aux(paf_file_t *pf, paf_rec_t *pr);

#ifdef __cplusplus
}
#endif

#endif
