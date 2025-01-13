/*********************************************************************************
 * MIT License                                                                   *
 *                                                                               *
 * Copyright (c) 2021 Chenxi Zhou <chnx.zhou@gmail.com>                          *
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
 * 15/04/21 - Chenxi Zhou: Created                                               *
 *                                                                               *
 *********************************************************************************/
#include <stdio.h>
#include <string.h>
#include <zlib.h>

#include "khash.h"
#include "ksort.h"
#include "kseq.h"
#include "kvec.h"

#include "sdict.h"

#undef DEBUG_DICT

KSEQ_INIT(gzFile, gzread)

void *kopen(const char *fn, int *_fd);
int kclose(void *a);

sdict_t *sd_init(void)
{
    sdict_t *d;
    d = (sdict_t *) calloc(1, sizeof(sdict_t));
    d->n = 0;
    d->m = 16;
    d->s = (sd_seq_t *) malloc(d->m * sizeof(sd_seq_t));
    d->h = kh_init(sdict);
    return d;
}

void sd_destroy(sdict_t *d)
{
    if (!d) return;
    uint32 i;
    if (d == 0)
        return;
    if (d->h)
        kh_destroy(sdict, d->h);
    if(d->s) {
        for (i = 0; i < d->n; ++i) {
            free(d->s[i].name);
            if (d->s[i].seq)
                free(d->s[i].seq);
        }
        free(d->s);
    }
    free(d);
}

uint32 sd_put(sdict_t *d, const char *name, uint32 len)
{
    if (!name)
        return UINT32_MAX;
    sdhash_t *h = d->h;
    khint_t k;
    int absent;
    k = kh_put(sdict, h, name, &absent);
    if (absent) {
        sd_seq_t *s;
        if (d->n == d->m) {
            d->m = d->m? d->m<<1 : 16;
            d->s = (sd_seq_t *) realloc(d->s, d->m * sizeof(sd_seq_t));
        }
        s = &d->s[d->n];
        s->len = len;
        s->seq = 0;
        kh_key(h, k) = s->name = strdup(name);
        kh_val(h, k) = d->n++;
    }
    return kh_val(h, k);
}

uint32 sd_put1(sdict_t *d, const char *name, const char *seq, uint32 len)
{
    uint32 k = sd_put(d, name, len);
    d->s[k].seq = strdup(seq);
    return k;
}

uint32 sd_get(sdict_t *d, const char *name)
{
    sdhash_t *h = d->h;
    khint_t k;
    k = kh_get(sdict, h, name);
    return k == kh_end(h)? UINT32_MAX : kh_val(h, k);
}

void sd_hash(sdict_t *d)
{
    uint32 i;
    sdhash_t *h;
    if (d->h)
        return;
    d->h = h = kh_init(sdict);
    for (i = 0; i < d->n; ++i) {
        int absent;
        khint_t k;
        k = kh_put(sdict, h, d->s[i].name, &absent);
        kh_val(h, k) = i;
    }
}

sdict_t *make_sdict_from_fa(const char *f, uint32 min_len)
{
    int fd;
    int64 l;
    gzFile fp;
    kseq_t *ks;
    void *ko = 0;

    ko = kopen(f, &fd);
    if (ko == 0) {
        fprintf(stderr, "[E::%s] cannot open file %s for reading\n", __func__, f);
        exit(EXIT_FAILURE);
    }
    fp = gzdopen(fd, "r");
    ks = kseq_init(fp);
    
    sdict_t *d;
    d = sd_init();
    while ((l = kseq_read(ks)) >= 0) {
        if (l > UINT32_MAX) {
            fprintf(stderr, "[E::%s] >4G sequence chunks are not supported: %s [%lld]\n", __func__, ks->name.s, l);
            exit(EXIT_FAILURE);
        }
        if (strlen(ks->seq.s) >= min_len)
            sd_put1(d, ks->name.s, ks->seq.s, strlen(ks->seq.s));
    }

    kseq_destroy(ks);
    gzclose(fp);
    kclose(ko);

    return d;
}

sdict_t *make_sdict_from_index(const char *f, uint32 min_len)
{
    iostream_t *fp;
    char *line;
    char name[4096];
    int64 len;

    fp = iostream_open(f);
    if (fp == NULL) {
        fprintf(stderr, "[E::%s] cannot open file %s for reading\n", __func__, f);
        exit(EXIT_FAILURE);
    }
    
    sdict_t *d;
    d = sd_init();
    while ((line = iostream_getline(fp)) != NULL) {
        if (is_empty_line(line))
            continue;
        sscanf(line, "%s %lld", name, &len);
        if (len > UINT32_MAX) {
            fprintf(stderr, "[E::%s] >4G sequence chunks are not supported: %s [%lld]\n", __func__, name, len);
            exit(EXIT_FAILURE);
        }
        if (len >= min_len)
            sd_put(d, name, len);
    }

    iostream_close(fp);
    return d;
}

sdict_t *make_sdict_from_gfa(const char *f, uint32 min_len)
{
    iostream_t *fp;
    char *line;
    char name[4096], lens[4096];
    uint64 len;

    fp = iostream_open(f);
    if (fp == NULL) {
        fprintf(stderr, "[E::%s] cannot open file %s for reading\n", __func__, f);
        exit(EXIT_FAILURE);
    }

    sdict_t *d;
    d = sd_init();
    while ((line = iostream_getline(fp)) != NULL) {
        if (is_empty_line(line))
            continue;
        if (line[0] == 'S') {
            sscanf(line, "%*s %s %*s %s", name, lens);
            len = strtoul(lens + 5, NULL, 10);
            if (len > UINT32_MAX) {
                fprintf(stderr, "[E::%s] >4G sequence chunks are not supported: %s [%lld]\n", __func__, name, len);
                exit(EXIT_FAILURE);
            }
            if (len >= min_len)
                sd_put(d, name, len);
        }
    }
    iostream_close(fp);

    return d;
}

int cmp_uint64_d (const void *a, const void *b) {
    // decreasing order
    uint64 x, y;
    x = *(uint64 *) a;
    y = *(uint64 *) b;
    return (x < y) - (x > y);
}

static void nl_stats(uint64 *s, uint32 n, uint64 *n_stats, uint32 *l_stats)
{
    uint32 i, j;
    double b, a, bs;
    
    bs = 0;
    for (i = 0; i < n; ++i)
        bs += s[i];

    qsort(s, n, sizeof(uint64), cmp_uint64_d);

    b = 0;
    j = 0;
    a = .1 * (++j) * bs;
    for (i = 0; i < n; ++i) {
        b += s[i];
        while (b >= a) {
            l_stats[j - 1] = i + 1;
            n_stats[j - 1] = s[i];
            a = .1 * (++j) * bs;
        }
    }
}

void sd_stats(sdict_t *d, uint64 *n_stats, uint32 *l_stats)
{
    // n_stats and l_stats are of at least size 10
    uint32 i, n;
    uint64 *s;

    n = d->n;
    s = (uint64 *) calloc(n, sizeof(uint64));
    for (i = 0; i < n; ++i)
        s[i] = d->s[i].len;
    nl_stats(s, n, n_stats, l_stats);
    
    free(s);
}
