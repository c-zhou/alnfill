/*********************************************************************************
 * MIT License                                                                   *
 *                                                                               *
 * Copyright (c) 2022 Chenxi Zhou <chnx.zhou@gmail.com>                          *
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
 * 03/08/22 - Chenxi Zhou: Created                                               *
 *                                                                               *
 *********************************************************************************/
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <zlib.h>

#include "ketopt.h"
#include "kvec.h"
#include "kseq.h"
#include "kthread.h"
#include "kstring.h"

#include "sdict.h"
#include "misc.h"
#include "paf.h"

#define ALNFILL_VERSION "0.1"

KSEQ_INIT(gzFile, gzread)

int VERBOSE = 0;

typedef struct {
    uint32 qsid, tsid;
    int64  qbeg, qend;
    int64  tbeg, tend;
    int    qbol, qeol;
    int    tbol, teol;
} interval_t;

typedef struct {
    interval_t *intervals;
    FILE **tmpfds;
    char **cmds;
    char **tfiles;
    char **qfiles;
    char **pfiles;
    sdict_t *tdicts;
    sdict_t *qdicts;
} data_t;

int run_system_cmd(char *cmd, int retry)
{ 
    int exit_code = system(cmd);
    --retry;
    if ((exit_code != -1 && !WEXITSTATUS(exit_code)) || !retry)
        return (exit_code);
    return (run_system_cmd(cmd, retry));
}

void check_executable(char *exe)
{
    char cmd[4096];
    sprintf(cmd, "command -v %s 1>/dev/null 2>/dev/null", exe);

    int exit_code = run_system_cmd(cmd, 1);
    if (exit_code == -1 || WEXITSTATUS(exit_code)) {
        fprintf(stderr, "[E::%s] executable %s is not available\n", __func__, exe);
        exit (1);
    }
}

static pthread_mutex_t print_mutex;

static inline int paf_parse1(int l, char *s, int64 qlen, int64 qbeg, int64 tlen, int64 tbeg, FILE *out)
{ 
    char *q;
	int i, t;
    while (isspace(*s)) s++;
    if (*s == '\0') return -1;
	for (i = t = 0, q = s; i <= l; ++i) {
		if (i < l && s[i] != '\t') continue;
		s[i] = 0;
        switch (t) {
            case 0:
            case 4:
            case 5:
                fprintf(out, "%s\t", q);
                break;
            case 1:
                fprintf(out, "%lld\t", qlen);
                break;
            case 2:
            case 3:
                fprintf(out, "%lld\t", strtol(q, NULL, 10) + qbeg);
                break;
            case 6:
                fprintf(out, "%lld\t", tlen);
                break;
            case 7:
            case 8:
                fprintf(out, "%lld\t", strtol(q, NULL, 10) + tbeg);
                break;
            case 9:
                s[i] = '\t';
                fprintf(out, "%s\n", q);
                return 0;
        }
		++t, q = i < l? &s[i+1] : 0;
	}
	return 0;
}

static inline int paf_read1(paf_file_t *pf, int64 qlen, int64 qbeg, int64 tlen, int64 tbeg, FILE *out)
{
	int ret, dret;
file_read_more:
	ret = ks_getuntil((kstream_t*)pf->fp, KS_SEP_LINE, &pf->buf, &dret);
	if (ret < 0) return ret;
	ret = paf_parse1(pf->buf.l, pf->buf.s, qlen, qbeg, tlen, tbeg, out);
	if (ret < 0) goto file_read_more;
	return ret;
}

void lastz_fill(void *_data, long i, int tid)
{
    data_t *data = (data_t *) _data;
    interval_t *interval = &data->intervals[i];
    uint32 tsid, qsid;
    int64 tlen, tbeg, qlen, qbeg;
    FILE *tfile, *qfile, *tmpfd;
    paf_file_t *pfile;

    tbeg = interval->tbeg;
    qbeg = interval->qbeg;
    tsid = interval->tsid;
    qsid = interval->qsid;
    tlen = data->tdicts->s[tsid].len;
    qlen = data->qdicts->s[qsid].len;

    tfile = fopen(data->tfiles[tid], "w");
    qfile = fopen(data->qfiles[tid], "w");
    if (tfile == NULL || qfile == NULL) {
        fprintf(stderr, "[E::%s] [thread %d] failed to open files to write\n", __func__, tid);
        exit (1);
    }
    
    fprintf(tfile, ">%s\n", data->tdicts->s[tsid].name);
    fwrite(data->tdicts->s[tsid].seq + interval->tbeg, sizeof(char), interval->tend - interval->tbeg, tfile);
    fputc('\n', tfile);
    fprintf(qfile, ">%s\n", data->qdicts->s[qsid].name);
    fwrite(data->qdicts->s[qsid].seq + interval->qbeg, sizeof(char), interval->qend - interval->qbeg, qfile);
    fputc('\n', qfile);

    if (fclose(tfile) || fclose(qfile)) {
        fprintf(stderr, "[E::%s] [thread %d] failed to close files\n", __func__, tid);
        exit (1);
    }
    
    if (run_system_cmd(data->cmds[tid], 1)) {
        fprintf(stderr, "[E::%s] [thread %d] failed to execute system command: %s\n", __func__, tid, data->cmds[tid]);
        exit (1);
    } else {
        pfile = paf_open(data->pfiles[tid]);
        if (!pfile) {
            fprintf(stderr, "[E::%s] [thread %d] cannot open paf file to read: %s\n", __func__, tid, data->pfiles[tid]);
            exit (1);
        }
        
        tmpfd = data->tmpfds[tid];
        while (paf_read1(pfile, qlen, qbeg, tlen, tbeg, tmpfd) >= 0);

        if (paf_close(pfile)) {
            fprintf(stderr, "[E::%s] [thread %d] failed to close file: %s\n", __func__, tid, data->pfiles[tid]);
            exit (1);
        }
    }

    if (unlink(data->tfiles[tid]) == -1 || 
        unlink(data->qfiles[tid]) == -1 ||
        unlink(data->pfiles[tid]) == -1 ) {
        fprintf(stderr, "[E::%s] [thread %d] failed to remove files\n", __func__, tid);
        exit (1);
    }

    if (i % 10000 == 0) {
        pthread_mutex_lock(&print_mutex);
        fprintf(stderr, "[M::%s] [thread %d] processed %ld intervals\n", __func__, tid, i);
        pthread_mutex_unlock(&print_mutex);
    }
}

static inline int parse_interval(int l, char *s, char **qname, int64 *qbeg, int64 *qend, char **tname, int64 *tbeg, int64 *tend, 
    int *qbol, int *qeol, int *tbol, int *teol)
{
    int i, fields;
    char *q;
	
    *qbol = 0;
    *qeol = 0;
    *tbol = 0;
    *teol = 0;
    i = fields = 0;
    
    // qname
    while (*s && isspace(*s) && i < l) {s++; i++;}
    q = s;
    while (*s && !isspace(*s) && i < l) {s++; i++;}
    if (s > q) {*s='\0'; *qname=q; fields++;} else {return fields;}

    // qbeg
    s++; i++;
    while (*s && isspace(*s) && i < l) {s++; i++;}
    q = s;
    while (*s && !isspace(*s) && i < l) {s++; i++;}
    if (s > q) {*s='\0'; *qbeg=strtoll(q,0,10); fields++;} else {return fields;}

    // qend
    s++; i++;
    while (*s && isspace(*s) && i < l) {s++; i++;}
    q = s;
    while (*s && !isspace(*s) && i < l) {s++; i++;}
    if (s > q) {*s='\0'; *qend=strtoll(q,0,10); fields++;} else {return fields;}
    
    // tname
    s++; i++;
    while (*s && isspace(*s) && i < l) {s++; i++;}
    q = s;
    while (*s && !isspace(*s) && i < l) {s++; i++;}
    if (s > q) {*s='\0'; *tname=q; fields++;} else {return fields;}
    
    // tbeg
    s++; i++;
    while (*s && isspace(*s) && i < l) {s++; i++;}
    q = s;
    while (*s && !isspace(*s) && i < l) {s++; i++;}
    if (s > q) {*s='\0'; *tbeg=strtoll(q,0,10); fields++;} else {return fields;}
    
    // tend
    s++; i++;
    while (*s && isspace(*s) && i < l) {s++; i++;}
    q = s;
    while (*s && !isspace(*s) && i < l) {s++; i++;}
    if (s > q) {*s='\0'; *tend=strtoll(q,0,10); fields++;} else {return fields;}

    // qbol
    s++; i++;
    while (*s && isspace(*s) && i < l) {s++; i++;}
    q = s;
    while (*s && !isspace(*s) && i < l) {s++; i++;}
    if (s > q) {*s='\0'; *qbol=strtol(q,0,10); fields++;} else {return fields;}

    // qeol
    s++; i++;
    while (*s && isspace(*s) && i < l) {s++; i++;}
    q = s;
    while (*s && !isspace(*s) && i < l) {s++; i++;}
    if (s > q) {*s='\0'; *qeol=strtol(q,0,10); fields++;} else {return fields;}

    // tbol
    s++; i++;
    while (*s && isspace(*s) && i < l) {s++; i++;}
    q = s;
    while (*s && !isspace(*s) && i < l) {s++; i++;}
    if (s > q) {*s='\0'; *tbol=strtol(q,0,10); fields++;} else {return fields;}

    // teol
    s++; i++;
    while (*s && isspace(*s) && i < l) {s++; i++;}
    q = s;
    while (*s && !isspace(*s) && i < l) {s++; i++;}
    if (s > q) {*s='\0'; *teol=strtol(q,0,10); fields++;} else {return fields;}

	return fields;
}

static ko_longopt_t long_options[] = {
    { "verbose",        ko_required_argument, 'v' },
    { "version",        ko_no_argument,       'V' },
    { "help",           ko_no_argument,       'h' },
    { 0, 0, 0 }
};

int main(int argc, char *argv[])
{
    const char *opt_str = "w:z:t:o:v:Vh";
    ketopt_t opt = KETOPT_INIT;
    int c, i, ret = 0;
    int n_threads;
    FILE *fp_help;
    kvec_t(interval_t) intervals;
    sdict_t *tdicts, *qdicts;
    char *workdir, *lazexec, *lazopts;

    sys_init();

    fp_help = stderr;
    workdir = "./";
    lazexec = "lastz";
    lazopts = "--format=PAF:wfmash --ambiguous=iupac";
    n_threads = 1;

    while ((c = ketopt(&opt, argc, argv, 1, opt_str, long_options)) >=0) {
        if (c == 't') n_threads = atoi(opt.arg);
        else if (c == 'w') workdir = opt.arg;
        else if (c == 'z') lazexec = opt.arg;
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
            puts(ALNFILL_VERSION);
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
        fprintf(fp_help, "Usage: alnfill [options] ref.fa[.gz] qry.fa[.gz] intervals\n");
        fprintf(fp_help, "Options:\n");
        fprintf(fp_help, "  -t INT               number of threads [%d]\n", n_threads);
        fprintf(fp_help, "  -w STR               work directory for temporary files [%s]\n", workdir);
        fprintf(fp_help, "  -z STR               lastz executable path [%s]\n", lazexec);
        fprintf(fp_help, "  -o FILE              write output to a file [stdout]\n");
        fprintf(fp_help, "  -v INT               verbose level [%d]\n", VERBOSE);
        fprintf(fp_help, "  --version            show version number\n");
        fprintf(fp_help, "\n");
        fprintf(fp_help, "Example: ./alnfill -t 32 -o gapfill.paf ref.fa qry.fa intervals.txt\n\n");
        return fp_help == stdout? 0 : 1;
    }

    if (argc - opt.ind < 3) {
        fprintf(stderr, "[E::%s] missing input: please specify three positional parameters\n", __func__);
        return 1;
    }

    check_executable(lazexec);

    tdicts = make_sdict_from_fa(argv[opt.ind],   0);
    qdicts = make_sdict_from_fa(argv[opt.ind+1], 0);

    kv_init(intervals);
    gzFile fp;
    kstream_t *ks;
    kstring_t buf = {0, 0, 0};
    char *qname, *tname;
    uint32 tsid, qsid;
    int64 qbeg, qend, tbeg, tend, tlen, qlen;
    int qbol, qeol, tbol, teol;
    int dret, fields;
    fp = gzopen(argv[opt.ind+2], "r");
    if (!fp) {
        fprintf(stderr, "[E::%s] failed to open file %s to read\n", __func__, argv[opt.ind+2]);
        exit (1);
    }
    ks = ks_init(fp);
    while (ks_getuntil(ks, KS_SEP_LINE, &buf, &dret) >= 0) {
        // header lines
        if (buf.l > 0 && buf.s[0] == '#') continue;

        fields = parse_interval(buf.l, buf.s, &qname, &qbeg, &qend, &tname, &tbeg, &tend, &qbol, &qeol, &tbol, &teol);
        
        if (fields < 6) {
            fprintf(stderr, "[W::%s] error reading interval line: %s...\n", __func__, buf.s);
            continue;
        }

        qsid = sd_get(qdicts, qname);
        if (qsid == UINT32_MAX) {
            fprintf(stderr, "[E::%s] query sequence not found: %s\n", __func__, qname);
            exit (1);
        }
        
        tsid = sd_get(tdicts, tname);
        if (tsid == UINT32_MAX) {
            fprintf(stderr, "[E::%s] target sequence not found: %s\n", __func__, tname);
            exit (1);
        }
        
        qlen = qdicts->s[qsid].len;
        tlen = tdicts->s[tsid].len;
        if (qbeg < qbol || qend + qeol > qlen || tbeg < tbol || tend + teol > tlen) {
            fprintf(stderr, "[W::%s] skip invalid gap: %s[%lld]:%lld[%d]-%lld[%d] x %s[%lld]:%lld[%d]-%lld[%d]\n", 
                __func__, qname, qlen, qbeg, qbol, qend, qeol, tname, tlen, tbeg, tbol, tend, teol);
            continue;
        }

        kv_push(interval_t, intervals, ((interval_t){qsid, tsid, qbeg, qend, tbeg, tend, qbol, qeol, tbol, teol}));
    }
    ks_destroy(ks);
    gzclose(fp);

    fprintf(stderr, "[M::%s] number of intervals to run: %ld\n", __func__, intervals.n);

    char *cmds[n_threads], *qfiles[n_threads], *tfiles[n_threads], *pfiles[n_threads], *template;
    FILE *tmpfds[n_threads];
    MYMALLOC(template, strlen(workdir)+35);
    for (i = 0; i < n_threads; i++) {
        sprintf(template, "%s/tempfileXXXXXX", workdir);
        int fd = mkstemp(template);
        if (fd == -1) {
            fprintf(stderr, "[E::%s] failed to make temporary file: %s\n", __func__, template);
            exit (1);
        }
        tmpfds[i] = fdopen(fd, "w");
        if (tmpfds[i] == NULL) {
            fprintf(stderr, "[E::%s] failed to open file to write: %s\n", __func__, template);
            close(fd);
            exit (1);
        }
        if (unlink(template) == -1) {
            fprintf(stderr, "[E::%s] failed to remove temporary file %s\n", __func__, template);
            fclose(tmpfds[i]);
            exit (1);
        }
        buf.l = 0; ksprintf(&buf, "%s_O.paf", template); pfiles[i] = strdup(buf.s);
        buf.l = 0; ksprintf(&buf, "%s_A.fna", template); tfiles[i] = strdup(buf.s);
        buf.l = 0; ksprintf(&buf, "%s_B.fna", template); qfiles[i] = strdup(buf.s);
        buf.l = 0; ksprintf(&buf, "%s %s --output=%s %s %s", lazexec, lazopts, pfiles[i], tfiles[i], qfiles[i]);
        cmds[i] = strdup(buf.s);
    }
    free(template);
    free(buf.s);

    if (VERBOSE > 0) {
        for (i = 0; i < n_threads; i++) {
            fprintf(stderr, "[M::%s] [thread %d] %s\n", __func__, i, tfiles[i]);
            fprintf(stderr, "[M::%s] [thread %d] %s\n", __func__, i, qfiles[i]);
            fprintf(stderr, "[M::%s] [thread %d] %s\n", __func__, i, pfiles[i]);
            fprintf(stderr, "[M::%s] [thread %d] %s\n", __func__, i, cmds[i]);
        }
    }

    data_t *data;
    MYCALLOC(data, 1);
    data->intervals = intervals.a;
    data->cmds = cmds;
    data->tmpfds = tmpfds;
    data->tfiles = tfiles;
    data->qfiles = qfiles;
    data->pfiles = pfiles;
    data->tdicts = tdicts;
    data->qdicts = qdicts;

    kt_for(n_threads, lastz_fill, data, intervals.n);

#define PUSH_BLOCK 0x100000
    char *buffer;
    MYMALLOC(buffer, PUSH_BLOCK);
    for (i = 0; i < n_threads; i++) {
        FILE *pfile = tmpfds[i];
        rewind(pfile);
        int x = PUSH_BLOCK;
        while (x >= PUSH_BLOCK)
          { x = fread(buffer,1,PUSH_BLOCK,pfile);
            if (x > 0)
              fwrite(buffer,x,1,stdout);
          }
        fclose(pfile);
    }
    free(buffer);

    for (i = 0; i < n_threads; i++) {
        free(data->tfiles[i]);
        free(data->qfiles[i]);
        free(data->pfiles[i]);
        free(data->cmds[i]);
    }
    free(data);
    kv_destroy(intervals);
    sd_destroy(tdicts);
    sd_destroy(qdicts);
    
    if (ret) {
        fprintf(stderr, "[E::%s] failed to analysis the PAF file\n", __func__);
        exit(EXIT_FAILURE);
    }

    if (fflush(stdout) == EOF) {
        fprintf(stderr, "[E::%s] failed to write the results\n", __func__);
        exit(EXIT_FAILURE);
    }

    if (VERBOSE >= 0) {
        fprintf(stderr, "[M::%s] Version: %s\n", __func__, ALNFILL_VERSION);
        fprintf(stderr, "[M::%s] CMD:", __func__);
        int i;
        for (i = 0; i < argc; ++i)
            fprintf(stderr, " %s", argv[i]);
        fprintf(stderr, "\n[M::%s] Real time: %.3f sec; CPU: %.3f sec; Peak RSS: %.3f GB\n", __func__, realtime() - realtime0, cputime(), peakrss() / 1024.0 / 1024.0 / 1024.0);
    }

    return 0;
}
