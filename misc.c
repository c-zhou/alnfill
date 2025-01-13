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
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <ctype.h>
#include <unistd.h>
#include <zlib.h>
#include <sys/resource.h>
#include <sys/time.h>

#include "kseq.h"
#include "misc.h"

KSTREAM_INIT(gzFile, gzread, BUFF_SIZE)

void *kopen(const char *fn, int *_fd);
int kclose(void *a);

#define ARRAY_SIZE(a) (sizeof(a) / sizeof((a)[0]))

double cputime(void)
{
    struct rusage r;
    getrusage(RUSAGE_SELF, &r);
    return r.ru_utime.tv_sec + r.ru_stime.tv_sec + 1e-6 * (r.ru_utime.tv_usec + r.ru_stime.tv_usec);
}

#ifdef __linux__
void liftrlimit()
{
    struct rlimit r;
    getrlimit(RLIMIT_AS, &r);
    r.rlim_cur = r.rlim_max;
    setrlimit(RLIMIT_AS, &r);
}
#else
void liftrlimit() {}
#endif

long peakrss(void)
{
    struct rusage r;
    getrusage(RUSAGE_SELF, &r);
#ifdef __linux__
    return r.ru_maxrss * 1024;
#else
    return r.ru_maxrss;
#endif
}

double realtime(void)
{
    struct timeval tp;
    gettimeofday(&tp, NULL);
    return tp.tv_sec + tp.tv_usec * 1e-6;
}

double realtime0;

void sys_init(void)
{
    liftrlimit();
    realtime0 = realtime();
}


#if (defined __APPLE__ && defined __MACH__) || defined __FreeBSD__ || defined __NetBSD__ || defined __OpenBSD__
#include <sys/sysctl.h>
#endif

long physmem_total(void)
{
    #if defined _SC_PHYS_PAGES && defined _SC_PAGESIZE
    {
        /* This works on linux-gnu, solaris2 and cygwin */
        long pages = sysconf(_SC_PHYS_PAGES);
        long pagesize = sysconf(_SC_PAGESIZE);
        if (0 <= pages && 0 <= pagesize)
        return pages * pagesize;
    }
    #elif defined CTL_HW && defined HW_PHYSMEM
    { 
        /* This works on *bsd and darwin */
        unsigned int physmem;
        size_t len = sizeof physmem;
        static int mib[2] = { CTL_HW, HW_PHYSMEM };
        if (sysctl (mib, ARRAY_SIZE(mib), &physmem, &len, NULL, 0) == 0
            && len == sizeof (physmem))
        return physmem;
    }
    #endif

    return 0;
}

long physmem_avail(void)
{
    #if defined _SC_AVPHYS_PAGES && defined _SC_PAGESIZE
    {
        /* This works on linux-gnu, solaris2 and cygwin */
        long pages = sysconf(_SC_AVPHYS_PAGES);
        long pagesize = sysconf(_SC_PAGESIZE);
        if (0 <= pages && 0 <= pagesize)
        return pages * pagesize;
    }
    #elif defined CTL_HW && defined HW_USERMEM
    {
        /* This works on *bsd and darwin */
        unsigned int usermem;
        size_t len = sizeof usermem;
        static int mib[2] = { CTL_HW, HW_USERMEM };
        if (sysctl (mib, ARRAY_SIZE(mib), &usermem, &len, NULL, 0) == 0
            && len == sizeof (usermem))
        return usermem;
    }
    #endif

    return 0;
}

void ram_limit(long *total, long *avail)
{
    long t, a, a1;
    struct rlimit lim;
    getrlimit(RLIMIT_RSS, &lim);
    a = lim.rlim_cur;
    getrlimit(RLIMIT_AS, &lim);
    a = MAX(a, (long) lim.rlim_cur);
    // a1 = (long) (sysconf(_SC_AVPHYS_PAGES) * sysconf(_SC_PAGESIZE));
    a1 = physmem_avail();
    a = a > 0? MIN(a, a1) : a1;
    // t = sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGESIZE);
    t = physmem_total();
    *avail = a;
    *total = t;
}

int strcmp_case_insensitive(const char *s1, const char *s2)
{
    const unsigned char *p1 = (const unsigned char *) s1;
    const unsigned char *p2 = (const unsigned char *) s2;
    int result;
    if (p1 == p2)
        return 0;
    while ((result = tolower(*p1) - tolower(*p2++)) == 0)
        if (*p1++ == '\0')
            break;
    return result;
}

int strncmp_case_insensitive(const char *s1, const char *s2, size_t n)
{
    const unsigned char *p1 = (const unsigned char *) s1;
    const unsigned char *p2 = (const unsigned char *) s2;
    int result;
    if (p1 == p2)
        return 0;
    result = 0;
    while ((n--) > 0 && (result = tolower(*p1) - tolower(*p2++)) == 0)
        if (*p1++ == '\0')
            break;
    return result;
}

void positive_or_die(int num)
{
    if (num <= 0 ) {
        fprintf(stderr, "[E::%s] nonpositive numeric error: %d\n", __func__, num);
        exit(EXIT_FAILURE);
    }
}

void mem_alloc_error(const char *obj)
{
    fprintf(stderr, "[E::%s] memory allocation error: %s\n", __func__, obj);
    exit(EXIT_FAILURE);
}

int is_empty_line(char *line)
{
    while (isspace(*line))
        line++;
    if(*line == '\0')
        return 1;
    return 0;
}

static kstring_t *kstring_init(int size)
{
    kstring_t *str;
    str = (kstring_t *) calloc(1, sizeof(kstring_t));
    str->m = size;
    str->s = (char *) malloc(sizeof(char) * size);
    return str;
}

static void kstring_destroy(kstring_t *str)
{
    if (!str) return;
    free(str->s);
    free(str);
}

iostream_t *iostream_open(const char *spath)
{
    iostream_t *iostream;
    iostream = (iostream_t *) calloc(1, sizeof(iostream_t));
    if (iostream == NULL)
        return NULL;
    iostream->koaux = kopen(spath, &iostream->fd);
    if (iostream->koaux == 0) {
        free(iostream);
        return NULL;
    }
    iostream->fp = gzdopen(iostream->fd, "r");
    if (iostream->fp == Z_NULL) {
        kclose(iostream->koaux);
        free(iostream);
        return NULL;
    }
    iostream->stream = ks_init(iostream->fp);
    iostream->buffer = kstring_init(BUFF_SIZE);

    return iostream;
}

void iostream_close(iostream_t *iostream)
{
    if (!iostream) return;
    ks_destroy(iostream->stream);
    kstring_destroy(iostream->buffer);
    gzclose(iostream->fp);
    kclose(iostream->koaux);
    free(iostream);
}

char *iostream_getline(iostream_t *iostream)
{
    int len;
    kstring_t *buffer;
    buffer = (kstring_t *) (iostream->buffer);
    len = ks_getuntil(iostream->stream, KS_SEP_LINE, buffer, 0);
    if (len >= 0) {
        iostream->nline += 1;
        return buffer->s;
    }
    return NULL;
}

#ifdef IOSTREAM_MAIN
int main(int argc, char *argv[])
{
    iostream_t *iostream = iostream_open(argv[1]);
    if (iostream == NULL) {
        fprintf(stdout, "[E::%s] cannot open file %s to read\n", __func__, argv[1]);
        exit (1);
    }
    char *line;
    while ((line = iostream_getline(iostream)) != NULL)
        fprintf(stdout, "[I::%s] Line %9ld: %s\n", __func__, iostream->nline, line);
    iostream_close(iostream);
}

#endif
