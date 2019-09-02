/***********************************************************************
* Copyright (c) 2008-2080 pepstack.com, 350137278@qq.com
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
 * rdbexpr.c
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-06-14
 * @update:
 */
#include "rdbcommon.h"

/**
 * https://github.com/kokke/tiny-regex-c
 */
#include "common/tiny-regex-c/re.h"


#define RDBEXPR_SGN(a, b)  ((a)>(b)? 1:((a)<(b)? (-1):0))


// src $expr dst ? true(1) : false(0)
//
int RDBExprValues (RDBValueType vt, const char *src, int slen, RDBFilterExpr expr, const char *dst, int dlen)
{
    // error
    int result = -1;
    int cmp;

    if (expr == RDBFIL_IGNORE) {
        // always accept if expr not given
        return 1;
    }

    if (vt == RDBVT_STR || ! vt) {
        cmp = cstr_compare_len(src, slen, dst, dlen);

        switch (expr) {
        case RDBFIL_EQUAL:
            result = (!cmp? 1 : 0);
            break;

        case RDBFIL_NOT_EQUAL:
            result = (!cmp? 0 : 1);
            break;

        case RDBFIL_GREAT_THAN:
            result = (cmp > 0? 1 : 0);
            break;

        case RDBFIL_LESS_THAN:
            result = (cmp < 0? 1 : 0);
            break;

        case RDBFIL_GREAT_EQUAL:
            result = (cmp >= 0? 1 : 0);
            break;

        case RDBFIL_LESS_EQUAL:
            result = (cmp <= 0? 1 : 0);
            break;

        case RDBFIL_LEFT_LIKE:
            // a like 'left%'
            //   src="aaaaB"
            //   dst="aaaaBBBB"    
            result = cstr_startwith(dst, dlen, src, slen);
            break;

        case RDBFIL_RIGHT_LIKE:
            // a like '%right'
            //   src="aBBBB"            
            //   dst="aaaaBBBB" 
            result = cstr_endwith(dst, dlen, src, slen);
            break;

        case RDBFIL_LIKE:
            // a like 'left%' or '%mid%' or '%right'
            result = cstr_containwith(dst, dlen, src, slen);
            break;

        case RDBFIL_MATCH:
            if (re_match(dst, src) == -1) {
                result = 0;
            } else {
                result = 1;
            }
            break;
        }

        return result;
    }

    if (vt == RDBVT_SB8 ||
        vt == RDBVT_SB4 ||
        vt == RDBVT_SB2 ||
        vt == RDBVT_CHAR) {
        sb8 s8src, s8dst;
        if (cstr_to_sb8(10, src, slen, &s8src) > 0 && cstr_to_sb8(10, dst, dlen, &s8dst) > 0) {
            cmp = RDBEXPR_SGN(s8src, s8dst);
        } else {
            // error
            return (-1);
        }
    } else if (vt == RDBVT_UB8 ||
        vt == RDBVT_UB4 ||
        vt == RDBVT_UB2 ||
        vt == RDBVT_BYTE) {
        ub8 u8src, u8dst;
        if (cstr_to_ub8(10, src, slen, &u8src) > 0 && cstr_to_ub8(10, dst, dlen, &u8dst) > 0) {
            cmp = RDBEXPR_SGN(u8src, u8dst);
        } else {
            // error
            return (-1);
        }
    } else if (vt == RDBVT_UB8X ||
        vt == RDBVT_UB4X) {
        ub8 u8src, u8dst;
        if (cstr_to_ub8(16, src, slen, &u8src) > 0 && cstr_to_ub8(16, dst, dlen, &u8dst) > 0) {
            cmp = RDBEXPR_SGN(u8src, u8dst);
        } else {
            // error
            return (-1);
        }
    } else if (vt == RDBVT_FLT64) {
        double dblsrc, dbldst;
        if (cstr_to_dbl(src, slen, &dblsrc) > 0 && cstr_to_dbl(dst, dlen, &dbldst) > 0) {
            cmp = RDBEXPR_SGN(dblsrc, dbldst);
        } else {
            // error
            return (-1);
        }
    } else {
        return (-1);
    }

    switch (expr) {
    case RDBFIL_EQUAL:
        result = (!cmp? 1 : 0);
        break;

    case RDBFIL_NOT_EQUAL:
        result = (! cmp? 0 : 1);
        break;

    case RDBFIL_GREAT_THAN:
        result = (cmp > 0? 1 : 0);
        break;

    case RDBFIL_LESS_THAN:
        result = (cmp < 0? 1 : 0);
        break;

    case RDBFIL_GREAT_EQUAL:
        result = (cmp >= 0? 1 : 0);
        break;

    case RDBFIL_LESS_EQUAL:
        result = (cmp <= 0? 1 : 0);
        break;
    }

    return result;
}
