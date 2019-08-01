/***********************************************************************
* Copyright (c) 2008-2080 syna-tech.com, pepstack.com, 350137278@qq.com
*
* ALL RIGHTS RESERVED.
* 
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions
* are met:
* 
*   Redistributions of source code must retain the above copyright
*    notice, this list of conditions and the following disclaimer.
* 
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
* "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
* LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
* A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
* OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
* SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
* LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
* DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
* THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
* OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
***********************************************************************/

/**
 * randctx.h
 *
 *   Standard definitions and types, Bob Jenkins
 *
 * 2015-01-19: revised by cheungmine
 */
#ifndef RANDCTX_H_INCLUDED
#define RANDCTX_H_INCLUDED

#if defined(__cplusplus)
extern "C"
{
#endif

#include "unitypes.h"

#define RANDSIZL   (8)
#define RANDSIZ    (1<<RANDSIZL)

/**
* context of random number generator
*/
struct randctx_ub4
{
    ub4 randcnt;
    ub4 seed[RANDSIZ];
    ub4 mm[RANDSIZ];
    ub4 aa;
    ub4 bb;
    ub4 cc;
};

typedef  struct randctx_ub4  randctx;


/**
* context of random number generator for 64-bits int
*/
struct randctx_ub8
{
    ub8 randcnt;
    ub8 seed[RANDSIZ];
    ub8 mm[RANDSIZ];
    ub8 aa;
    ub8 bb;
    ub8 cc;
};

typedef struct randctx_ub8  randctx64;

/**
* randinit
*   init rand seed
*/
static void randctx_init(randctx *r, ub4 seed);

static void randctx64_init(randctx64 *r, ub8 seed);

/**
* rand
*   Call rand(randctx *) to retrieve a single 32-bit random value.
*/
static ub4 rand_gen(randctx *r);

static ub4 rand_gen_int(randctx *r, ub4 rmin, ub4 rmax);

static ub8 rand64_gen(randctx64 *r);

static ub8 rand64_gen_int(randctx64 *r, ub8 rmin, ub8 rmax);

/**
* gray code api
*   test OK !
*   https://blog.csdn.net/jingfengvae/article/details/51691124
*/
static ub4 binary2graycode(ub4 x);

static ub4 graycode2binary(ub4 x);

static ub8 binary2graycode64(ub8 x);

static ub8 graycode2binary64(ub8 x);


/**
*==============================================================================
* 32-bits int random generator
*==============================================================================
*/
#define isaac_golden_ratio32  0x9e3779b9

#define ind32(mm,x)  (*(ub4 *)((ub1 *)(mm) + ((x) & ((RANDSIZ-1)<<2))))

#define rngstep32(mix,a,b,mm,m,m2,r,x) do { \
        x = *m;  \
        a = (a^(mix)) + *(m2++); \
        *(m++) = y = ind32(mm,x) + a + b; \
        *(r++) = b = ind32(mm,y>>RANDSIZL) + x; \
    } while(0)


#define mix32(a,b,c,d,e,f,g,h) do { \
        a^=b<<11; d+=a; b+=c; \
        b^=c>>2;  e+=b; c+=d; \
        c^=d<<8;  f+=c; d+=e; \
        d^=e>>16; g+=d; e+=f; \
        e^=f<<10; h+=e; f+=g; \
        f^=g>>4;  a+=f; g+=h; \
        g^=h<<8;  b+=g; h+=a; \
        h^=a>>9;  c+=h; a+=b; \
    } while(0)


static void isaac32(randctx *ctx)
{
    register ub4 a, b, x, y, *m, *mm, *m2, *r, *mend;
    mm = ctx->mm;
    r = ctx->seed;
    a = ctx->aa;
    b = ctx->bb + (++ctx->cc);

    for (m = mm, mend = m2 = m+(RANDSIZ/2); m<mend; ) {
        rngstep32( a<<13, a, b, mm, m, m2, r, x);
        rngstep32( a>>6 , a, b, mm, m, m2, r, x);
        rngstep32( a<<2 , a, b, mm, m, m2, r, x);
        rngstep32( a>>16, a, b, mm, m, m2, r, x);
    }

    for (m2 = mm; m2<mend; ) {
        rngstep32( a<<13, a, b, mm, m, m2, r, x);
        rngstep32( a>>6 , a, b, mm, m, m2, r, x);
        rngstep32( a<<2 , a, b, mm, m, m2, r, x);
        rngstep32( a>>16, a, b, mm, m, m2, r, x);
    }

    ctx->bb = b;
    ctx->aa = a;
}


void randctx_init(randctx *ctx, ub4 seed)
{
    int i;
    ub4 a, b, c, d, e, f, g, h;
    ub4 *m, *r;
    ctx->aa = ctx->bb = ctx->cc = 0;
    m = ctx->mm;
    r = ctx->seed;
    a = b = c = d = e = f = g = h = isaac_golden_ratio32; /* the golden ratio */

    /* init seed */
    for (i=0; i<256; ++i) {
        r[i] = seed;
    }

    for (i=0; i<4; ++i) {                                 /* scramble it */
        mix32(a,b,c,d,e,f,g,h);
    }

    if (1) {
        /* initialize using the contents of r[] as the seed */
        for (i=0; i<RANDSIZ; i+=8) {
            a+=r[i  ]; b+=r[i+1]; c+=r[i+2]; d+=r[i+3];
            e+=r[i+4]; f+=r[i+5]; g+=r[i+6]; h+=r[i+7];
            mix32(a,b,c,d,e,f,g,h);
            m[i  ]=a; m[i+1]=b; m[i+2]=c; m[i+3]=d;
            m[i+4]=e; m[i+5]=f; m[i+6]=g; m[i+7]=h;
        }

        /* do a second pass to make all of the seed affect all of m */
        for (i=0; i<RANDSIZ; i+=8) {
            a+=m[i  ]; b+=m[i+1]; c+=m[i+2]; d+=m[i+3];
            e+=m[i+4]; f+=m[i+5]; g+=m[i+6]; h+=m[i+7];
            mix32(a,b,c,d,e,f,g,h);
            m[i  ]=a; m[i+1]=b; m[i+2]=c; m[i+3]=d;
            m[i+4]=e; m[i+5]=f; m[i+6]=g; m[i+7]=h;
        }
    } else {
        /* Never run to this: fill in m[] with messy stuff */
        for (i=0; i<RANDSIZ; i+=8) {
            mix32(a,b,c,d,e,f,g,h);
            m[i  ]=a; m[i+1]=b; m[i+2]=c; m[i+3]=d;
            m[i+4]=e; m[i+5]=f; m[i+6]=g; m[i+7]=h;
        }
    }

    isaac32(ctx);              /* fill in the first set of results */
    ctx->randcnt = RANDSIZ;  /* prepare to use the first set of results */
}


/**
* rand_gen
*   get 32-bits unsigned integer random
*/
ub4 rand_gen(randctx *r)
{
    return (!(r)->randcnt-- ? \
            (isaac32(r), (r)->randcnt=RANDSIZ-1, (r)->seed[(r)->randcnt]) : \
            (r)->seed[(r)->randcnt]);
}


/**
* rand_gen_int
*   get integer random between rmin and rmax
*/
ub4 rand_gen_int(randctx *r, ub4 rmin, ub4 rmax)
{
    if (! r->randcnt-- ) {
        isaac32(r);
        r->randcnt = RANDSIZ - 1;
    }

    ub4 ret = (ub4) r->seed[r->randcnt];

    return ret % (ub4) (rmax - rmin + 1) + rmin;
}


/**
*==============================================================================
* 64 bits int random generator
*==============================================================================
*/
#define isaac_golden_ratio64  0x9e3779b97f4a7c13LL

#define ind64(mm,x)  (*(ub8 *)((ub1 *)(mm) + ((x) & ((RANDSIZ-1)<<3))))

#define rngstep64(mix,a,b,mm,m,m2,r,x) do { \
        x = *m;  \
        a = (mix) + *(m2++); \
        *(m++) = y = ind64(mm,x) + a + b; \
        *(r++) = b = ind64(mm,y>>RANDSIZL) + x; \
    } while(0)

#define mix64(a,b,c,d,e,f,g,h) do { \
        a-=e; f^=h>>9;  h+=a; \
        b-=f; g^=a<<9;  a+=b; \
        c-=g; h^=b>>23; b+=c; \
        d-=h; a^=c<<15; c+=d; \
        e-=a; b^=d>>14; d+=e; \
        f-=b; c^=e<<20; e+=f; \
        g-=c; d^=f>>17; f+=g; \
        h-=d; e^=g<<14; g+=h; \
    } while(0)

static void isaac64(randctx64 *ctx)
{
    register ub8 a,b,x,y,*m,*mm,*m2,*r,*mend;
    mm = ctx->mm;
    r = ctx->seed;
    a = ctx->aa;
    b = ctx->bb + (++ctx->cc);

    for (m = mm, mend = m2 = m+(RANDSIZ/2); m<mend; ) {
        rngstep64(~(a^(a<<21)), a, b, mm, m, m2, r, x);
        rngstep64(  a^(a>>5)  , a, b, mm, m, m2, r, x);
        rngstep64(  a^(a<<12) , a, b, mm, m, m2, r, x);
        rngstep64(  a^(a>>33) , a, b, mm, m, m2, r, x);
    }

    for (m2 = mm; m2<mend; ) {
        rngstep64(~(a^(a<<21)), a, b, mm, m, m2, r, x);
        rngstep64(  a^(a>>5)  , a, b, mm, m, m2, r, x);
        rngstep64(  a^(a<<12) , a, b, mm, m, m2, r, x);
        rngstep64(  a^(a>>33) , a, b, mm, m, m2, r, x);
    }
    ctx->bb = b;
    ctx->aa = a;
}


void randctx64_init(randctx64 *ctx, ub8 seed)
{
    int i;
    ub8 a,b,c,d,e,f,g,h;
    ub8 *mm, *r;
    ctx->aa = ctx->bb = ctx->cc = (ub8) 0;

    a=b=c=d=e=f=g=h= isaac_golden_ratio64;  /* the golden ratio */

    mm = ctx->mm;
    r = ctx->seed;

    /* init seed */
    for (i=0; i<256; ++i) {
        r[i] = seed;
    }

    for (i=0; i<4; ++i) {                   /* scramble it */
        mix64(a,b,c,d,e,f,g,h);
    }

    for (i=0; i<RANDSIZ; i+=8) {  /* fill in mm[] with messy stuff */
        if (1) {               /* use all the information in the seed */
            a+=r[i  ]; b+=r[i+1]; c+=r[i+2]; d+=r[i+3];
            e+=r[i+4]; f+=r[i+5]; g+=r[i+6]; h+=r[i+7];
        }
        mix64(a,b,c,d,e,f,g,h);
        mm[i  ]=a; mm[i+1]=b; mm[i+2]=c; mm[i+3]=d;
        mm[i+4]=e; mm[i+5]=f; mm[i+6]=g; mm[i+7]=h;
    }

    if (1) {
        /* do a second pass to make all of the seed affect all of mm */
        for (i=0; i<RANDSIZ; i+=8) {
            a+=mm[i  ]; b+=mm[i+1]; c+=mm[i+2]; d+=mm[i+3];
            e+=mm[i+4]; f+=mm[i+5]; g+=mm[i+6]; h+=mm[i+7];
            mix64(a,b,c,d,e,f,g,h);
            mm[i  ]=a; mm[i+1]=b; mm[i+2]=c; mm[i+3]=d;
            mm[i+4]=e; mm[i+5]=f; mm[i+6]=g; mm[i+7]=h;
        }
    }

    isaac64(ctx);             /* fill in the first set of results */
    ctx->randcnt = RANDSIZ;   /* prepare to use the first set of results */
}


/**
* rand64_gen
*   get 64-bits unsigned integer random
*/
ub8 rand64_gen(randctx64 *r)
{
    return (ub8) (!(r)->randcnt-- ? (isaac64(r), (r)->randcnt=RANDSIZ-1, (r)->seed[(r)->randcnt]) : \
            (r)->seed[(r)->randcnt]);
}


/**
* rand64_gen_int
*   get 64-bits unsigned integer random
*/
ub8 rand64_gen_int(randctx64 *r, ub8 rmin, ub8 rmax)
{
    if (! r->randcnt-- ) {
        isaac64(r);
        r->randcnt = RANDSIZ - 1;
    }

    ub8 ret = (ub8) r->seed[r->randcnt];

    return ret % (ub8) (rmax - rmin + 1) + rmin;
}


/**
 * binary2graycode
 *   Convert an unsigned binary number to reflected binary Gray code.
 */
ub4 binary2graycode(ub4 x)
{
    return (x >> 1) ^ x;
}


/**
 * graycode2binary
 *   Convert a reflected binary Gray code number to a binary number.
 */
ub4 graycode2binary(ub4 x)
{
    x ^= x >> 16;
    x ^= x >> 8;
    x ^= x >> 4;
    x ^= x >> 2;
    x ^= x >> 1;

    return x;
}


ub8 binary2graycode64(ub8 x)
{
    ub8 h = binary2graycode((ub4) ((x & 0xffffffff00000000) >> 32));
    ub8 l = binary2graycode((ub4) (x & 0x00000000ffffffff));

    return (ub8) ((h << 32) | l);
}


ub8 graycode2binary64(ub8 g)
{
    ub8 h = graycode2binary((ub4) ((g & 0xffffffff00000000) >> 32));
    ub8 l = graycode2binary((ub4) (g & 0x00000000ffffffff));

    return (ub8) ((h << 32) | l);
}

#ifdef __cplusplus
}
#endif

#endif /* RANDCTX_H_INCLUDED */
