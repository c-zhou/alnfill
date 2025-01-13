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
#ifndef SDICT_H_
#define SDICT_H_

#include <stdint.h>

#include "khash.h"
#include "misc.h"

extern char comp_table[128];
extern char nucl_toupper[128];

typedef struct {
    char *name; // seq id
    char *seq; // sequence
    uint32 len; // seq length
} sd_seq_t;

KHASH_MAP_INIT_STR(sdict, uint32)
typedef khash_t(sdict) sdhash_t;

typedef struct {
    uint32 n, m; // n: seq number, m: memory allocated
    sd_seq_t *s; // sequence dictionary
    sdhash_t *h; // sequence hash map: name -> index
} sdict_t;

typedef enum cc_error_code {
    CC_SUCCESS = 0,
    SEQ_NOT_FOUND = 1,
    POS_NOT_IN_RANGE = 2
} CC_ERR_t;

#ifdef __cplusplus
extern "C" {
#endif

sdict_t *sd_init(void);
void sd_destroy(sdict_t *d);
uint32 sd_put(sdict_t *d, const char *name, uint32 len);
uint32 sd_put1(sdict_t *d, const char *name, const char *seq, uint32 len);
uint32 sd_get(sdict_t *d, const char *name);
sdict_t *make_sdict_from_fa(const char *f, uint32 min_len);
sdict_t *make_sdict_from_index(const char *f, uint32 min_len);
sdict_t *make_sdict_from_gfa(const char *f, uint32 min_len);
void sd_stats(sdict_t *d, uint64 *n_stats, uint32 *l_stats);
#ifdef __cplusplus
}
#endif

#endif /* SDICT_H_ */

