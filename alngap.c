/*********************************************************************************
 * MIT License                                                                   *
 *                                                                               *
 * Copyright (c) 2024 Chenxi Zhou <chnx.zhou@gmail.com>                          *
 *                                                                               *
 * Permission is hereby granted, free of charge, to any person obtaining a copy  *
 * of this software and associated documentation files (the "Software"), to deal *
 * in the Software without restriction, including without limitation the rights  *
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell     *
 * copies of the Software, and to permit persons to whom the Software is         *
 * furnished to do so, subject to the following conditions:                      *
 *                                                                               *
 * The above copyright notice and this permission notice shall be included in    *
 * all copies or substantial portions of the Software.                           *
 *                                                                               *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR    *
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,      *
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE   *
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER        *
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, *
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE *
 * SOFTWARE.                                                                     *
 *********************************************************************************/

/********************************** Revision History *****************************
 *                                                                               *
 * 19/12/24 - Chenxi Zhou: Created                                               *
 *                                                                               *
 *********************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <pthread.h>

#include "ketopt.h"
#include "kvec.h"
#include "kthread.h"

#include "paf.h"
#include "misc.h"
#include "sdict.h"
#include "rtree.h"

#define ALNGAP_VERSION "0.1"

int VERBOSE = 0;

typedef struct {
    int    aread, bread;
    int64  abpos, aepos;
    int64  bbpos, bepos;
    int    mlen;
} aln_t;

typedef struct {
    int beg;
    int end;
} range_t;

aln_t *read_pafs(char **fs, int fn, sdict_t *qdicts, sdict_t *tdicts, int64 *_naln)
{
    paf_file_t *paf;
    paf_rec_t _rec, *rec;
    uint32 qid, tid;
    kvec_t(aln_t) alns;
    int i;

    kv_init(alns);
    kv_resize(aln_t, alns, 1<<24);
    rec = &_rec;
    for (i = 0; i < fn; i++) {
        paf = paf_open(fs[i]);
        if (!paf) {
            fprintf(stderr, "[E::%s] cannot open paf file to read: %s\n", __func__, fs[i]);
            exit (1);
        }
        while (paf_read(paf, rec) >= 0) {
            qid = sd_put(qdicts, rec->qn, rec->ql);
            tid = sd_put(tdicts, rec->tn, rec->tl);
            kv_push(aln_t, alns, ((aln_t){qid, tid, rec->qs, rec->qe, rec->ts, rec->te, rec->ml}));
            if (alns.n % 1000000 == 0)
                fprintf(stderr, "[M::%s] read %ld paf records\n", __func__, alns.n);
        }
        paf_close(paf);
    }
    fprintf(stderr, "[M::%s] read %ld paf records\n", __func__, alns.n);

    MYREALLOC(alns.a, alns.n);

    *_naln = alns.n;
    return (alns.a);
}

typedef kvec_t(range_t) rangeset_t;

static inline int overlap(int b1, int e1, int b2, int e2)
{
    int max, min;
    max = MAX(b1, b2);
    min = MIN(e1, e2);
    if (max < min)
        return (min - max);
    return 0;
}

int find_last_small(range_t *ranges, int n, int val)
{
    int low = 0, high = n - 1, mid;
    int res = -1;
    while (low <= high) {
        mid = low + (high - low) / 2;
        if (ranges[mid].end < val) {
            res = mid;
            low = mid + 1;
        } else
            high = mid - 1;
    }
    return res;
}

int find_first_large(range_t *ranges, int n, int val)
{
    int low = 0, high = n - 1, mid;
    int res = n;
    while (low <= high) {
        mid = low + (high - low) / 2;
        if (ranges[mid].beg > val) {
            res = mid;
            high = mid - 1;
        } else
            low = mid + 1;
    }
    return res;
}

int rangeset_overlap(rangeset_t *spans, int beg, int end, int *_b, int *_e)
{   
    int b, e, o;
    *_b = -1;
    *_e = spans->n;
    if (beg >= end || spans->n == 0) return 0;
    b = find_last_small(spans->a, spans->n, beg);
    e = find_first_large(spans->a, spans->n, end);
    *_b = b;
    *_e = e;
    o = 0;
    while (++b < e)
        o += overlap(spans->a[b].beg, spans->a[b].end, beg, end);
    return o;
}

void rangeset_add(rangeset_t *spans, int beg, int end, int b, int e)
{
    int nels;
    if (beg >= end) return;
    // expected number of elements
    // left    :   b + 1
    // overlap :   1
    // right   :   spans->n - e
    nels = spans->n + b - e + 2;
    // span the rangeset if needed
    if (spans->m < nels) 
        kv_resize(range_t, *spans, nels);
    if (b+1 < spans->n)
        beg = MIN(spans->a[b+1].beg, beg);
    if (e-1 < spans->n)
        end = MAX(spans->a[e-1].end, end);
    if (spans->n > e)
        memmove(spans->a+b+2, spans->a+e, sizeof(range_t)*(spans->n-e));
    spans->a[b+1] = (range_t) {beg, end};
    spans->n = nels;
}

static int MORDER(const void *a, const void *b)
{
    int x, y;
    x = ((aln_t *) a)->mlen;
    y = ((aln_t *) b)->mlen;
    return (x < y) - (x > y);
}

static void coverage_summary(rangeset_t *ranges, int n, int64 *_ns, int64 *_nb)
{
    int i, j;
    int64 ns, nb;
    range_t *range;
    ns = 0;
    nb = 0;
    for (i = 0; i < n; i++) {
        ns += ranges[i].n;
        range = ranges[i].a;
        for (j = ranges[i].n; j; ) {
            --j;
            nb += range[j].end - range[j].beg;
        }
    }
    *_ns = ns;
    *_nb = nb;
}

aln_t *reciprocal_best_aligns(aln_t *alns, int64 naln, sdict_t *qdicts, sdict_t *tdicts, double max_cov, int64 *_naln)
{
    aln_t *aln;
    rangeset_t *q_span, *t_span;
    int qo, to, qs, qe, ts, te;
    double mo;
    int64 i, n_rec, ns, nb;
    
    fprintf(stderr, "[M::%s] selecting reciprocal best alignments from %lld records\n", __func__, naln);

    qsort(alns, naln, sizeof(aln_t), MORDER);

    MYCALLOC(q_span, qdicts->n + tdicts->n);
    if (q_span == NULL)
        mem_alloc_error("q&t spans");
    t_span = q_span + qdicts->n;
    n_rec = 0;
    for (i = 0; i < naln; i++) {
        aln = alns + i;
        qo = rangeset_overlap(q_span + aln->aread, aln->abpos, aln->aepos, &qs, &qe);
        to = rangeset_overlap(t_span + aln->bread, aln->bbpos, aln->bepos, &ts, &te);
        mo = aln->mlen * max_cov;
        if (qo <= mo && to <= mo) {
            // keep the alignment
            rangeset_add(q_span + aln->aread, aln->abpos, aln->aepos, qs, qe);
            rangeset_add(t_span + aln->bread, aln->bbpos, aln->bepos, ts, te);
            ++n_rec;
        } else aln->mlen = 0;
        if ((i+1) % 1000000 == 0)
            fprintf(stderr, "[M::%s] processed %lld records, %lld selected\n", __func__, i+1, n_rec);
    }
    fprintf(stderr, "[M::%s] processed %lld records, %lld selected\n", __func__, naln, n_rec);

    coverage_summary(q_span, qdicts->n, &ns, &nb);
    fprintf(stderr, "[M::%s] query genome covered with %lld segments of %lld bases\n", __func__, ns, nb);
    coverage_summary(t_span, tdicts->n, &ns, &nb);
    fprintf(stderr, "[M::%s] target genome covered with %lld segments of %lld bases\n", __func__, ns, nb);

    for (i = qdicts->n + tdicts->n - 1; i >= 0; i--)
        kv_destroy(q_span[i]);
    free(q_span);

    n_rec = 0;
    for (i = 0; i < naln; i++)
        if (alns[i].mlen > 0)
            alns[n_rec++] = alns[i];
    *_naln = n_rec;
    MYREALLOC(alns, n_rec);
    
    return alns;
}

static int RORDER(const void *a, const void *b)
{ 
    int64  xm, ym;
    aln_t *x = (aln_t *) a;
    aln_t *y = (aln_t *) b;
    
    xm = x->aread;
    ym = y->aread;

    if (xm == ym) {
        xm = x->bread;
        ym = y->bread;
    } else return ((xm > ym) - (xm < ym));

    if (xm == ym) {
        xm = x->abpos;
        ym = y->abpos;
    } else return ((xm > ym) - (xm < ym));

    if (xm == ym) {
        xm = x->bbpos;
        ym = y->bbpos;
    } else return ((xm > ym) - (xm < ym));

    if (xm == ym) {
        xm = x->aepos;
        ym = y->aepos;
    } else return ((xm > ym) - (xm < ym));

    if (xm == ym) {
        xm = x->bepos;
        ym = y->bepos;
    }
    return ((xm > ym) - (xm < ym));
}

static int AORDER(const void *a, const void *b)
{ 
    int64  xm, ym;
    aln_t *x = (aln_t *) a;
    aln_t *y = (aln_t *) b;
    
    xm = (x->aepos - x->abpos) * (x->bepos - x->bbpos);
    ym = (y->aepos - y->abpos) * (y->bepos - y->bbpos);
    
    return ((xm > ym) - (xm < ym));
}

bool item_clone(const DATATYPE item, DATATYPE *into, void *udata) {return true;}
void item_free(const DATATYPE item, void *udata) {};

typedef kvec_t(aln_t) aln_v;

typedef struct {
    int min_gap, max_gap, max_ovl;
    aln_t   *alns;
    aln_v   *abufs;
    aln_v   *gbufs;
    uint64  *ranges;
    sdict_t *tdicts;
    sdict_t *qdicts;
} data_t;

static pthread_mutex_t print_mutex;

static int64 b_stats[] = {0, 0, 0, 0};

void gap_core(void *_data, long i, int tid)
{
    data_t *data = (data_t *) _data;
    int max_gap = data->max_gap;
    int min_gap = data->min_gap;
    int max_ovl = data->max_ovl;
    aln_v *gaps = &data->gbufs[tid];
    int64  naln = (uint32) data->ranges[i];
    aln_t *alns = data->abufs[tid].a;
    memcpy(alns+1, data->alns+(data->ranges[i]>>32), sizeof(aln_t)*naln);
    const char *qname = data->qdicts->s[alns[1].aread].name;
    const char *tname = data->tdicts->s[alns[1].bread].name;

    int64 abpos1, aepos1, bbpos1, bepos1;
    int64 abpos2, aepos2, bbpos2, bepos2;
    int64 alen, blen, bound, dist;
    aln_t *aln1, *aln2, *aln1e, *aln2s, *aln2e;

    // add two ends
    alns[0] = (aln_t) {0, 0, 0, 0, 0, 0, 0};
    alen = data->qdicts->s[alns[1].aread].len;
    blen = data->tdicts->s[alns[1].bread].len;
    alns[naln+1] = (aln_t) {0, 0, alen, alen, blen, blen, 0};
    naln += 2;

    // find gaps
    gaps->n = 0;
    aln1e = alns + naln;
    for (aln1 = alns; aln1 < aln1e; aln1++) {
        abpos1 = aln1->abpos;
        aepos1 = aln1->aepos;
        bbpos1 = aln1->bbpos;
        bepos1 = aln1->bepos;
        bound  = aepos1 + min_gap;
        aln2s  = aln1 + 1;
        while (aln2s < aln1e && aln2s->abpos < bound) {aln2s++;}
        bound  = aepos1 + max_gap;
        aln2e = aln2s;
        while (aln2e < aln1e && aln2e->abpos < bound) {aln2e++;}
        for (aln2 = aln2s; aln2 < aln2e; aln2++) {
            abpos2 = aln2->abpos;
            aepos2 = aln2->aepos;
            bbpos2 = aln2->bbpos;
            bepos2 = aln2->bepos;
            dist   = (bbpos1>bbpos2? bbpos1 : bbpos2) - (bepos1<bepos2? bepos1 : bepos2);
            if (dist >= min_gap && dist <= max_gap)
                kv_push(aln_t, *gaps,
                    ((aln_t) {
                        0, 
                        0,
                        (abpos1>aepos1-max_ovl)? abpos1 : (aepos1-max_ovl), 
                        (aepos2<abpos2+max_ovl)? aepos2 : (abpos2+max_ovl), 
                        (bepos1<bepos2? ((bbpos1>bepos1-max_ovl)? bbpos1 : (bepos1-max_ovl)) : ((bbpos2>bepos2-max_ovl)? bbpos2 : (bepos2-max_ovl))), 
                        (bbpos1>bbpos2? ((bepos1<bbpos1+max_ovl)? bepos1 : (bbpos1+max_ovl)) : ((bepos2<bbpos2+max_ovl)? bepos2 : (bbpos2+max_ovl))),
                        0
                    }));
        }
    }
    if (!gaps->n) return;

    // only keep minimal bounding boxes, i. e., those spanning a single gap
    qsort(gaps->a, gaps->n, sizeof(aln_t), AORDER); // sort by size
    
    struct rtree *gap_tr = rtree_new();
    gap_tr->item_clone = item_clone;
    gap_tr->item_free  = item_free;
    for (aln1 = gaps->a, aln1e = gaps->a+gaps->n; aln1 < aln1e; aln1++) {
        if (!rtree_exist_node_inside(gap_tr, (NUMTYPE[2]){aln1->abpos,aln1->bbpos}, (NUMTYPE[2]){aln1->aepos,aln1->bepos})) {
            aln1->mlen = 1;
            rtree_insert(gap_tr, (NUMTYPE[2]){aln1->abpos,aln1->bbpos}, (NUMTYPE[2]){aln1->aepos,aln1->bepos}, NULL);
        }
    }
    rtree_free(gap_tr);

    // output gaps
    pthread_mutex_lock(&print_mutex);
    for (aln1 = gaps->a, aln1e = gaps->a+gaps->n; aln1 < aln1e; aln1++) {
        if (aln1->mlen == 0) continue;
        b_stats[0] += 1;
        b_stats[1] += aln1->aepos - aln1->abpos;
        b_stats[2] += aln1->bepos - aln1->bbpos;
        b_stats[3] += (aln1->aepos - aln1->abpos) * (aln1->bepos - aln1->bbpos);
        fprintf(stdout, "%s\t%lld\t%lld\t%s\t%lld\t%lld\n", qname, aln1->abpos, aln1->aepos, tname, aln1->bbpos, aln1->bepos);
    }
    pthread_mutex_unlock(&print_mutex);
}

static int align_gaps(aln_t *alns, int64 naln, sdict_t *qdicts, sdict_t *tdicts, int n_threads, int min_gap, int max_gap, int max_ovl)
{ 
    if (naln <= 0) return 0;

    int64 i, j, m;
    int a, b;
    kvec_t(uint64) ranges;
    aln_v  *abufs, *gbufs;
    data_t *data;

    qsort(alns, naln, sizeof(aln_t), RORDER);

    kv_init(ranges);
    a = alns->aread;
    b = alns->bread;
    m = 0;
    for (i = 1, j = 0; i < naln; i++) {
        if (alns[i].aread != a || alns[i].bread != b) {
            kv_push(uint64, ranges, (uint64)j<<32|(i-j));
            if (m < i-j) m = i-j;
            j = i;
            a = alns[j].aread;
            b = alns[j].bread;
        }
    }
    kv_push(uint64, ranges, (uint64)j<<32|(i-j));
    if (m < i-j) m = i-j;
    MYCALLOC(abufs, n_threads*2);
    if (abufs == NULL)
        mem_alloc_error("a&b bufs");
    gbufs = abufs + n_threads;
    for (i = 0; i < n_threads; i++) {
        kv_resize(aln_t, abufs[i], m+2);
        kv_resize(aln_t, gbufs[i], m*4);
        if (abufs[i].a == NULL || gbufs[i].a == NULL)
            mem_alloc_error("a/b bufs");
    }

    MYCALLOC(data, 1);
    data->min_gap = min_gap;
    data->max_gap = max_gap;
    data->max_ovl = max_ovl;
    data->alns  = alns;
    data->abufs = abufs;
    data->gbufs = gbufs;
    data->ranges = ranges.a;
    data->tdicts = tdicts;
    data->qdicts = qdicts;

    kt_for(n_threads, gap_core, data, ranges.n);

    for (i = 0; i < n_threads*2; i++)
        kv_destroy(abufs[i]);
    free(abufs);
    free(data);
    kv_destroy(ranges);

    fprintf(stderr, "[M::%s]: selected gap filling boxes: %lld; q_bases: %lld; t_bases: %lld; area: %lld\n", __func__, b_stats[0], b_stats[1], b_stats[2], b_stats[3]);

    return 0;
}

static inline int64 parse_num2(const char *str, char **q)
{
	double x;
	char *p;
	x = strtod(str, &p);
	if (*p == 'G' || *p == 'g') x *= 1e9, ++p;
	else if (*p == 'M' || *p == 'm') x *= 1e6, ++p;
	else if (*p == 'K' || *p == 'k') x *= 1e3, ++p;
	if (q) *q = p;
	return (int64_t)(x + .499);
}

static inline int64 parse_num(const char *str)
{
	return parse_num2(str, 0);
}

static ko_longopt_t long_options[] = {
    { "verbose",        ko_required_argument, 'v' },
    { "version",        ko_no_argument,       'V' },
    { "help",           ko_no_argument,       'h' },
    { 0, 0, 0 }
};

int main(int argc, char *argv[])
{ 
    const char *opt_str = "al:m:e:t:f:o:v:Vh";
    ketopt_t opt = KETOPT_INIT;
    int c, ret = 0;
    int n_threads;
    FILE *fp_help;
    sdict_t *tdicts, *qdicts;
    aln_t *alns;
    int64 naln;
    int min_gap, max_gap, max_ovl, do_rba;
    double max_cov;
    
    sys_init();

    fp_help = stderr;
    min_gap = 100;
    max_gap = 1000000;
    max_ovl = 1000;
    max_cov = 0.5;
    do_rba = 1;
    n_threads = 1;
  
    while ((c = ketopt(&opt, argc, argv, 1, opt_str, long_options)) >=0 ) {
        if (c == 'l') min_gap = (int) parse_num(opt.arg);
        else if (c == 'm') max_gap = (int) parse_num(opt.arg);
        else if (c == 'f') max_cov = (int) parse_num(opt.arg);
        else if (c == 'e') max_ovl = atoi(opt.arg);
        else if (c == 'a') do_rba = 0;
        else if (c == 't') n_threads = atoi(opt.arg);
        else if (c == 'o') {
            if (strcmp(opt.arg, "-") != 0) {
                if (freopen(opt.arg, "wb", stdout) == NULL) {
                    fprintf(stderr, "[ERROR]\033[1;31m failed to write the output to file '%s'\033[0m: %s\n", opt.arg, strerror(errno));
                    return 1;
                }
            }
        }
        else if (c == 'v') VERBOSE = atoi(opt.arg);
        else if (c == 'h') fp_help = stdout;
        else if (c == 'V') {
            puts(ALNGAP_VERSION);
            return 0;
        }
        else if (c == '?') {
            fprintf(stderr, "[E::%s] unknown option: \"%s\"\n", __func__, argv[opt.i - 1]);
            return 1;
        }
        else if (c == ':') {
            fprintf(stderr, "[E::%s] missing option: \"%s\"\n", __func__, argv[opt.i - 1]);
            return 1;
        }
    }

    if (argc == opt.ind || fp_help == stdout) {
        fprintf(fp_help, "\n");
        fprintf(fp_help, "Usage: alngap [options] input.paf[.gz]\n");
        fprintf(fp_help, "Options:\n");
        fprintf(fp_help, "  -l INT               min gap size to fill in [100]\n");
        fprintf(fp_help, "  -m INT               max gap size to fill in [1M]\n");
        fprintf(fp_help, "  -e INT               max flank sequence size [1K]\n");
        fprintf(fp_help, "  -a                   use all instead of reciprocal best alignments\n");
        fprintf(fp_help, "  -f INT               max overlap for reciprocal best alignments [%.1f]\n", max_cov);
        fprintf(fp_help, "  -t INT               number of threads [%d]\n", n_threads);
        fprintf(fp_help, "  -o FILE              write output to a file [stdout]\n");
        fprintf(fp_help, "  -v INT               verbose level [%d]\n", VERBOSE);
        fprintf(fp_help, "  --version            show version number\n");
        fprintf(fp_help, "\n");
        fprintf(fp_help, "Example: ./alngap -o intervals.txt input.paf\n\n");
        return fp_help == stdout? 0 : 1;
    }

    if (argc - opt.ind < 1) {
        fprintf(stderr, "[E::%s] missing input: please specify at least one positional parameter\n", __func__);
        return 1;
    }

    // read PAF files
    qdicts = sd_init();
    tdicts = sd_init();
    
    alns = read_pafs(argv + opt.ind, argc - opt.ind, qdicts, tdicts, &naln);

    // find reciprocal best alignments
    if (do_rba)
        alns = reciprocal_best_aligns(alns, naln, qdicts, tdicts, max_cov, &naln);

    // find gaps
    ret = align_gaps(alns, naln, qdicts, tdicts, n_threads, min_gap, max_gap, max_ovl);
    
    sd_destroy(qdicts);
    sd_destroy(tdicts);
    free(alns);

    if (ret) {
        fprintf(stderr, "[E::%s] failed to analysis the PAF file\n", __func__);
        exit(EXIT_FAILURE);
    }

    if (fflush(stdout) == EOF) {
        fprintf(stderr, "[E::%s] failed to write the results\n", __func__);
        exit(EXIT_FAILURE);
    }

    if (VERBOSE >= 0) {
        fprintf(stderr, "[M::%s] Version: %s\n", __func__, ALNGAP_VERSION);
        fprintf(stderr, "[M::%s] CMD:", __func__);
        int i;
        for (i = 0; i < argc; ++i)
            fprintf(stderr, " %s", argv[i]);
        fprintf(stderr, "\n[M::%s] Real time: %.3f sec; CPU: %.3f sec; Peak RSS: %.3f GB\n", __func__, realtime() - realtime0, cputime(), peakrss() / 1024.0 / 1024.0 / 1024.0);
    }

    return 0;
}
