/*********************************************************************************
 * MIT License                                                                   *
 *                                                                               *
 * Copyright (c) 2025 Chenxi Zhou <chnx.zhou@gmail.com>                          *
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
 * 08/01/25 - Chenxi Zhou: Created                                               *
 *                                                                               *
 *********************************************************************************/
#ifndef MISC_H_
#define MISC_H_

#include <stdint.h>
#include <stdio.h>
#include <zlib.h>

extern double realtime0;

typedef unsigned char      uint8;
typedef unsigned short     uint16;
typedef unsigned int       uint32;
typedef unsigned long long uint64;
typedef signed char        int8;
typedef signed short       int16;
typedef signed int         int32;
typedef signed long long   int64;
typedef float              float32;
typedef double             float64;

#define BUFF_SIZE 4096

#ifndef kroundup32
#define kroundup32(x) (--(x), (x)|=(x)>>1, (x)|=(x)>>2, (x)|=(x)>>4, (x)|=(x)>>8, (x)|=(x)>>16, ++(x))
#endif
#ifndef kroundup64
#define kroundup64(x) (--(x), (x)|=(x)>>1, (x)|=(x)>>2, (x)|=(x)>>4, (x)|=(x)>>8, (x)|=(x)>>16, (x)|=(x)>>32, ++(x))
#endif

#define SWAP(T, x, y) {T tmp = x; x = y; y = tmp;}
#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))
#define MIN_MAX(x, min, max) MIN((MAX((x), (min))), (max))

#define MYMALLOC(ptr, len) ((ptr) = (__typeof__(ptr))malloc(sizeof(*(ptr)) * (len)))
#define MYCALLOC(ptr, len) ((ptr) = (__typeof__(ptr))calloc((len), sizeof(*(ptr))))
#define MYREALLOC(ptr, len) ((ptr) = (__typeof__(ptr))realloc((ptr), sizeof(*(ptr)) * (len)))
#define MYBZERO(ptr, len) memset((ptr), 0, sizeof(*(ptr)) * (len))
#define MYBONE(ptr, len) memset((ptr), 0xff, sizeof(*(ptr)) * (len))
#define MYEXPAND(a, m) do { \
    ++(m); \
    kroundup64((m)); \
    MYREALLOC((a), (m)); \
} while (0)

#define strcasecmp(s1, s2) strcmp_case_insensitive(s1, s2)

typedef struct {
    int fd;
    gzFile fp;
    void *stream;
    void *buffer;
    void *koaux;
    int64 nline;
} iostream_t;

#ifdef __cplusplus
extern "C" {
#endif
void sys_init(void);
double realtime(void);
double cputime(void);
void liftrlimit();
long peakrss(void);
void ram_limit(long *total, long *avail);
int strcmp_case_insensitive(const char *s1, const char *s2);
int strncmp_case_insensitive(const char *s1, const char *s2, size_t n);
void mem_alloc_error(const char *obj);
void positive_or_die(int num);
int is_empty_line(char *line);
iostream_t *iostream_open(const char *spath);
void iostream_close(iostream_t *iostream);
char *iostream_getline(iostream_t *iostream);
#ifdef __cplusplus
}
#endif

#endif /* MISC_H_ */

