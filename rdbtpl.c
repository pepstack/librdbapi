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
 * rdbtpl.c
 *
 * @author: master@pepstack.com
 *
 * @version: 1.0.0
 * @create: 2019-06-14
 * @update:
 */
#include "rdbresultmap.h"

/**
    tpl_bin outbin;

    RDBFieldDes_t fldes[2] = {0};

    fldes[0].namelen = snprintf_chkd_V1(fldes[0].fieldname, sizeof(fldes[0].fieldname), "entrytoken");
    fldes[0].fieldtype = RDBVT_UB8X;
    fldes[0].rowkey = 1;
    snprintf_chkd_V1(fldes[0].comment, sizeof(fldes[0].comment), "entry token in hex encoded bigint");

    fldes[1].namelen = snprintf_chkd_V1(fldes[1].fieldname, sizeof(fldes[1].fieldname), "fullname");
    fldes[1].fieldtype = RDBVT_STR;
    fldes[1].nullable = 1;
    snprintf_chkd_V1(fldes[1].comment, sizeof(fldes[1].comment), "path file fullname");

    if (RDBFieldDesPack(fldes, 2, &outbin) == 0) {
        RDBFieldDes_t outfldes[2] = {0};

        if (RDBFieldDesUnpack(outbin.addr, outbin.sz, outfldes, 2)) {
            int j;
            for (j = 0; j < 2; j++) {
                printf("fieldname=%s\n", outfldes[j].fieldname);
                printf("comment=%s\n", outfldes[j].comment);
            }
        }

        free(outbin.addr);
    }
 */

int RDBFieldDesPack (RDBFieldDes_t * infieldes, int numfields, tpl_bin *outbin)
{
    int j, rc = -1;

    tpl_node *tn;

    RDBFieldDesTpl fieldestpl = (RDBFieldDesTpl) RDBMemAlloc(sizeof(*fieldestpl) * numfields);

    for (j = 0; j < numfields; j++) {
        fieldestpl[j].fieldname = infieldes[j].fieldname;
        fieldestpl[j].namelen = infieldes[j].namelen;

        fieldestpl[j].comment = infieldes[j].comment;

        fieldestpl[j].rowkey = infieldes[j].rowkey;
        fieldestpl[j].nullable = infieldes[j].nullable;
        fieldestpl[j].dscale = infieldes[j].dscale;

        fieldestpl[j].length = infieldes[j].length;
        fieldestpl[j].fieldtype = infieldes[j].fieldtype;
    }

    tn = tpl_map(RDBFieldDesTplFmt"#", fieldestpl, numfields);
    if (tn) {
        if (tpl_pack(tn, 0) == 0) {
            rc = tpl_dump(tn, TPL_MEM, &outbin->addr, &outbin->sz);
        }

        tpl_free(tn);
    }

    RDBMemFree(fieldestpl);
    return rc;
}


int RDBFieldDesUnpack (const void *addrin, ub4 sizein, RDBFieldDes_t * outfieldes, int numfields)
{
    int rc = 0;

    tpl_node *tn;

    RDBFieldDesTpl fieldestpl = (RDBFieldDesTpl) RDBMemAlloc(sizeof(*fieldestpl) * numfields);

    tn = tpl_map(RDBFieldDesTplFmt"#", fieldestpl, numfields);

    if (tn) {
        if (tpl_load(tn, TPL_MEM, addrin, sizein) == 0) {
            rc = tpl_unpack(tn, 0);
            if (rc == 1) {
                int j;

                for (j = 0; j < numfields; j++) {
                    snprintf_chkd_V1(outfieldes[j].fieldname, sizeof(outfieldes[j].fieldname), "%.*s", fieldestpl[j].namelen, fieldestpl[j].fieldname);
                    snprintf_chkd_V1(outfieldes[j].comment, sizeof(outfieldes[j].comment), "%s", fieldestpl[j].comment);

                    outfieldes[j].rowkey = fieldestpl[j].rowkey;
                    outfieldes[j].nullable = fieldestpl[j].nullable;
                    outfieldes[j].dscale = fieldestpl[j].dscale;

                    outfieldes[j].length = fieldestpl[j].length;
                    outfieldes[j].fieldtype = fieldestpl[j].fieldtype;

                    free(fieldestpl[j].fieldname);
                    free(fieldestpl[j].comment);
                }
            }
        }
        tpl_free(tn);
    }

    RDBMemFree(fieldestpl);

    return rc;
}
