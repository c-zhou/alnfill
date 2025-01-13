CFLAGS=		-O3 -Wall -fno-strict-aliasing -Wno-unused-function -Wno-deprecated-declarations
CPPFLAGS=
INCLUDES=
OBJS=
PROG=		alnfill alngap
PROG_EXTRA=
LIBS=		-lm -lz -lpthread
DESTDIR=	~/bin

.PHONY: all extra clean depend test
.SUFFIXES:.c .o

.c.o:
		$(CC) -c $(CFLAGS) $(CPPFLAGS) $(INCLUDES) $< -o $@

all: $(PROG)

extra: all $(PROG_EXTRA)

debug: $(PROG)
debug: CFLAGS += -DDEBUG

alnfill: alnfill.o sdict.o paf.o misc.o kthread.o kalloc.o kopen.o
		$(CC) $(CFLAGS) $^ -o $@ -L. $(LIBS)

alngap: alngap.o sdict.o rtree.o paf.o misc.o kthread.o kalloc.o kopen.o
		$(CC) $(CFLAGS) $^ -o $@ -L. $(LIBS)

clean:
		rm -fr *.o a.out $(PROG) $(OBJS) $(PROG_EXTRA)

install:
		cp $(PROG) $(DESTDIR)

depend:
		(LC_ALL=C; export LC_ALL; makedepend -Y -- $(CFLAGS) $(CPPFLAGS) -- *.c)

# DO NOT DELETE

sdict.o: sdict.h misc.h khash.h ksort.h kseq.h kvec.h
paf.o: paf.h misc.h kseq.h
misc.o: misc.h kseq.h
kthread.o: kthread.h
kalloc.o: kalloc.h
alngap.o: sdict.h rtree.h misc.h paf.h ketopt.h kvec.h
alnfill.o: sdict.h misc.h paf.h ketopt.h kvec.h kseq.h kthread.h kstring.h
