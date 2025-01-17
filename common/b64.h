/*********************************************************************\

MODULE NAME:    b64

AUTHOR:         Bob Trower 08/04/01

PROJECT:        Crypt Data Packaging

COPYRIGHT:      Copyright (c) Trantor Standard Systems Inc., 2001

NOTE:           This source code may be used as you wish, subject to
                the MIT license.  See the LICENCE section below.

DESCRIPTION:
                This little utility implements the Base64
                Content-Transfer-Encoding standard described in
                RFC1113 (http://www.faqs.org/rfcs/rfc1113.html).

                This is the coding scheme used by MIME to allow
                binary data to be transferred by SMTP mail.

                Groups of 3 bytes from a binary stream are coded as
                groups of 4 bytes in a text stream.

                The input stream is 'padded' with zeros to create
                an input that is an even multiple of 3.

                A special character ('=') is used to denote padding so
                that the stream can be decoded back to its exact size.

                Encoded output is formatted in lines which should
                be a maximum of 72 characters to conform to the
                specification.  This program defaults to 72 characters,
                but will allow more or less through the use of a
                switch.  The program enforces a minimum line size
                of 4 characters.

                Example encoding:

                The stream 'ABCD' is 32 bits long.  It is mapped as
                follows:

                ABCD

                 A (65)     B (66)     C (67)     D (68)   (None) (None)
                01000001   01000010   01000011   01000100

                16 (Q)  20 (U)  9 (J)   3 (D)    17 (R) 0 (A)  NA (=) NA (=)
                010000  010100  001001  000011   010001 000000 000000 000000


                QUJDRA==

                Decoding is the process in reverse.  A 'decode' lookup
                table has been created to avoid string scans.

DESIGN GOALS:   Specifically:
        Code is a stand-alone utility to perform base64
        encoding/decoding. It should be genuinely useful
        when the need arises and it meets a need that is
        likely to occur for some users.
        Code acts as sample code to show the author's
        design and coding style.

        Generally:
        This program is designed to survive:
        Everything you need is in a single source file.
        It compiles cleanly using a vanilla ANSI C compiler.
        It does its job correctly with a minimum of fuss.
        The code is not overly clever, not overly simplistic
        and not overly verbose.
        Access is 'cut and paste' from a web page.
        Terms of use are reasonable.

VALIDATION:     Non-trivial code is never without errors.  This
                file likely has some problems, since it has only
                been tested by the author.  It is expected with most
                source code that there is a period of 'burn-in' when
                problems are identified and corrected.  That being
                said, it is possible to have 'reasonably correct'
                code by following a regime of unit test that covers
                the most likely cases and regression testing prior
                to release.  This has been done with this code and
                it has a good probability of performing as expected.

                Unit Test Cases:

                case 0:empty file:
                    CASE0.DAT  ->  ->
                    (Zero length target file created
                    on both encode and decode.)

                case 1:One input character:
                    CASE1.DAT A -> QQ== -> A

                case 2:Two input characters:
                    CASE2.DAT AB -> QUJD -> AB

                case 3:Three input characters:
                    CASE3.DAT ABC -> QUJD -> ABC

                case 4:Four input characters:
                    case4.dat ABCD -> QUJDRA== -> ABCD

                case 5:All chars from 0 to ff, linesize set to 50:

                    AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIj
                    JCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZH
                    SElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWpr
                    bG1ub3BxcnN0dXZ3eHl6e3x9fn+AgYKDhIWGh4iJiouMjY6P
                    kJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq+wsbKz
                    tLW2t7i5uru8vb6/wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX
                    2Nna29zd3t/g4eLj5OXm5+jp6uvs7e7v8PHy8/T19vf4+fr7
                    /P3+/w==

                case 6:Mime Block from e-mail:
                    (Data same as test case 5)

                case 7: Large files:
                    Tested 28 MB file in/out.

                case 8: Random Binary Integrity:
                    This binary program (b64.exe) was encoded to base64,
                    back to binary and then executed.

                case 9 Stress:
                    All files in a working directory encoded/decoded
                    and compared with file comparison utility to
                    ensure that multiple runs do not cause problems
                    such as exhausting file handles, tmp storage, etc.

                -------------

                Syntax, operation and failure:
                    All options/switches tested.  Performs as
                    expected.

                case 10:
                    No Args -- Shows Usage Screen
                    Return Code 1 (Invalid Syntax)
                case 11:
                    One Arg (invalid) -- Shows Usage Screen
                    Return Code 1 (Invalid Syntax)
                case 12:
                    One Arg Help (-?) -- Shows detailed Usage Screen.
                    Return Code 0 (Success -- help request is valid).
                case 13:
                    One Arg Help (-h) -- Shows detailed Usage Screen.
                    Return Code 0 (Success -- help request is valid).
                case 14:
                    One Arg (valid) -- Uses stdin/stdout (filter)
                    Return Code 0 (Sucess)
                case 15:
                    Two Args (invalid file) -- shows system error.
                    Return Code 2 (File Error)
                case 16:
                    Encode non-existent file -- shows system error.
                    Return Code 2 (File Error)
                case 17:
                    Out of disk space -- shows system error.
                    Return Code 3 (File I/O Error)

                -------------

                Compile/Regression test:
                    gcc compiled binary under Cygwin
                    Microsoft Visual Studio under Windows 2000
                    Microsoft Version 6.0 C under Windows 2000

DEPENDENCIES:   None

LICENCE:        Copyright (c) 2001 Bob Trower, Trantor Standard Systems Inc.

                Permission is hereby granted, free of charge, to any person
                obtaining a copy of this software and associated
                documentation files (the "Software"), to deal in the
                Software without restriction, including without limitation
                the rights to use, copy, modify, merge, publish, distribute,
                sublicense, and/or sell copies of the Software, and to
                permit persons to whom the Software is furnished to do so,
                subject to the following conditions:

                The above copyright notice and this permission notice shall
                be included in all copies or substantial portions of the
                Software.

                THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY
                KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
                WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
                PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
                OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
                OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
                OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
                SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

VERSION HISTORY:
                Bob Trower 08/04/01 -- Create Version 0.00.00B

LAST MODIFIED:  cheungmine@gmail.com 2009-11-2
\******************************************************************* */
/**
 * b64.h
 *   base 64 encode and decode
 *
 * Example:
 *
 *   int   size_out;
 *   int   size_ret;
 *   char  *b64_out;
 *   char  *b64_outB;
 *
 *   char  b64_in[] = "hello world shanghai";
 *
 *   // Base64 �����ַ���
 *   size_out = b64_encode_string((const unsigned char*)b64_in, (int)strlen(b64_in), 0, 0);
 *   b64_out = (char*) malloc(size_out+1);
 *   size_ret = b64_encode_string((const unsigned char*)b64_in, (int)strlen(b64_in), (unsigned char*)b64_out, 0);
 *   b64_out[size_ret] = 0;
 *   assert(size_ret==size_out);
 *
 *   // Base64 �����ַ���
 *   size_out = b64_decode_string((const unsigned char*)b64_out, size_ret, 0);
 *   b64_outB = (char*) malloc(size_out+1);
 *   size_ret = b64_decode_string((const unsigned char*)b64_out, size_ret, (unsigned char*)b64_outB);
 *   b64_outB[size_ret] = 0;
 *   assert(size_ret==size_out);
 *
 *   assert(strcmp(b64_outB, b64_in)==0);
 *
 *   free(b64_out);
 *   free(b64_outB);
 */

#ifndef _B64_H_INCLUDED
#define _B64_H_INCLUDED

#if defined(__cplusplus)
extern "C"
{
#endif

#include <stdio.h>
#include <stdlib.h>


#ifndef BASE64_DEF_LINE_SIZE
#  define BASE64_DEF_LINE_SIZE   72
#endif

#ifndef BASE64_MIN_LINE_SIZE
#  define BASE64_MIN_LINE_SIZE    4
#endif

#define B64_DEF_LINE_SIZE    BASE64_DEF_LINE_SIZE
#define B64_MIN_LINE_SIZE    BASE64_MIN_LINE_SIZE

/*
** Translation Table as described in RFC1113
*/
static const char cb64[]="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/*
** Translation Table to decode (created by author)
*/
static const char cd64[]="|$$$}rstuvwxyz{$$$$$$$>?@ABCDEFGHIJKLMNOPQRSTUVW$$$$$$XYZ[\\]^_`abcdefghijklmnopq";

/*
** encodeblock
**
** encode 3 8-bit binary bytes as 4 '6-bit' characters
*/
static void encodeblock( unsigned char in[3], unsigned char out[4], int len )
{
    out[0] = cb64[ in[0] >> 2 ];
    out[1] = cb64[ ((in[0] & 0x03) << 4) | ((in[1] & 0xf0) >> 4) ];
    out[2] = (unsigned char) (len > 1 ? cb64[ ((in[1] & 0x0f) << 2) | ((in[2] & 0xc0) >> 6) ] : '=');
    out[3] = (unsigned char) (len > 2 ? cb64[ in[2] & 0x3f ] : '=');
}

/*
** encode
**
** base64 encode a stream adding padding and line breaks as per spec.
*/
static void b64_encode_stream( FILE *infile, FILE *outfile, int linesize )
{
    unsigned char in[3], out[4];
    int i, len, blocksout = 0;

    while( !feof( infile ) ) {
        len = 0;
        for( i = 0; i < 3; i++ ) {
            in[i] = (unsigned char) getc( infile );
            if( !feof( infile ) ) {
                len++;
            }
            else {
                in[i] = 0;
            }
        }
        if( len ) {
            encodeblock( in, out, len );
            for( i = 0; i < 4; i++ ) {
                putc( out[i], outfile );
            }
            blocksout++;
        }

        if (linesize){
            if( blocksout >= (linesize/4) || feof( infile ) ) {
                if( blocksout ) {
                    fprintf( outfile, "\r\n" );
                }
                blocksout = 0;
            }
        }
    }
}

/*
** encode
**
** base64 encode bytes
** by cheungmine
** outstr is allocated by caller
** length of encoded string is returned (not including end closing '\0')
*/
static int b64_encode_string( const unsigned char *instr, int inlen, unsigned char* outstr, int linesize )
{
    unsigned char in[3], out[4];
    int inc=0, outc=0, i, len, blocksout = 0;

    /* only size of buffer required */
    if (!outstr){
        if (linesize==0)
            return (inlen%3==0)? ((inlen/3)*4):((inlen/3+1)*4);

        while(inc < inlen) {
            len = 0;
            for( i = 0; i < 3; i++ ) {
                if (inc < inlen){
                    len++;
                    inc++;
                }
            }
            if( len ) {
                outc += 4;
                blocksout++;
            }
            if( blocksout >= (linesize/4) || inc==inlen ) {
                if( blocksout ) {
                    outc += 2;
                }
                blocksout = 0;
            }
        }
        return outc;
    }

    while(inc < inlen) {
        len = 0;
        for( i = 0; i < 3; i++ ) {
            if (inc < inlen){
                len++;
                in[i] = instr[inc++];
            }
            else{
                in[i] = 0;  // padding with zero
            }
        }
        if( len ) {
            encodeblock( in, out, len );
            for( i = 0; i < 4; i++ ) {
                outstr[outc++] = out[i];
            }
            blocksout++;
        }

        if (linesize){
            if( blocksout >= (linesize/4) || inc==inlen ) {
                if( blocksout ) {
                    outstr[outc++] = '\r';
                    outstr[outc++] = '\n';
                }
                blocksout = 0;
            }
        }
    }
    return outc;
}

/*
** decodeblock
**
** decode 4 '6-bit' characters into 3 8-bit binary bytes
*/
static void decodeblock( unsigned char in[4], unsigned char out[3] )
{
    out[ 0 ] = (unsigned char ) (in[0] << 2 | in[1] >> 4);
    out[ 1 ] = (unsigned char ) (in[1] << 4 | in[2] >> 2);
    out[ 2 ] = (unsigned char ) (((in[2] << 6) & 0xc0) | in[3]);
}

/*
** decode
**
** decode a base64 encoded stream discarding padding, line breaks and noise
*/
static void b64_decode_stream( FILE *infile, FILE *outfile )
{
    unsigned char in[4], out[3], v;
    int i, len;

    while( !feof( infile ) ) {
        for( len = 0, i = 0; i < 4 && !feof( infile ); i++ ) {
            v = 0;
            while( !feof( infile ) && v == 0 ) {
                v = (unsigned char) getc( infile );
                v = (unsigned char) ((v < 43 || v > 122) ? 0 : cd64[ v - 43 ]);
                if( v ) {
                    v = (unsigned char) ((v == '$') ? 0 : v - 61);
                }
            }
            if( !feof( infile ) ) {
                len++;
                if( v ) {
                    in[ i ] = (unsigned char) (v - 1);
                }
            }
            else {
                in[i] = 0;
            }
        }
        if( len ) {
            decodeblock( in, out );
            for( i = 0; i < len - 1; i++ ) {
                putc( out[i], outfile );
            }
        }
    }
}

/*
 * add by cheungmine
 * 2009-11-1
 */
static int b64_decode_string( const unsigned char *instr, int inlen, unsigned char *outstr )
{
    unsigned char in[4], out[3], v;
    int inc=0, outc=0, i, len;

    /* only size of buffer required */
    if (!outstr){
        while( inc < inlen ) {
            for( len = 0, i = 0; i < 4 && inc<inlen; i++ ) {
                v = 0;
                while( (inc<=inlen) && (v==0) ) {
                    v = (inc < inlen)? instr[inc] : 0;
                    inc++;

                    v = (unsigned char) ((v < 43 || v > 122) ? 0 : cd64[ v - 43 ]);
                    if( v ) {
                        v = (unsigned char) ((v == '$') ? 0 : v - 61);
                    }
                }
                if( inc<=inlen ) {
                    len++;
                }
            }
            if( len>1 ) {
                outc += (len-1);
            }
        }
        return outc;
    }

    while( inc < inlen ) {
        for( len = 0, i = 0; i < 4 && inc<inlen; i++ ) {
            v = 0;
            while( (inc<=inlen) && (v==0) ) {
                v = (inc < inlen)? instr[inc] : 0;
                inc++;

                v = (unsigned char) ((v < 43 || v > 122) ? 0 : cd64[ v - 43 ]);
                if( v ) {
                    v = (unsigned char) ((v == '$') ? 0 : v - 61);
                }
            }
            if( inc<=inlen ) {
                len++;
                if( v ) {
                    in[ i ] = (unsigned char) (v - 1);
                }
            }
            else {
                in[i] = 0;
            }
        }
        if( len ) {
            decodeblock( in, out );
            for( i = 0; i < len - 1; i++ ) {
                outstr[outc++] = out[i];
            }
        }
    }
    return outc;
}


/*
 * add by cheungmine
 * 2010-6-18
 */
static int b64_decode_wstring( const wchar_t *instr, int inlen, unsigned char *outstr )
{
    unsigned char in[4], out[3], v;
    int inc=0, outc=0, i, len;

    /* only size of buffer required */
    if (!outstr){
        while( inc < inlen ) {
            for( len = 0, i = 0; i < 4 && inc<inlen; i++ ) {
                v = 0;
                while( (inc<=inlen) && (v==0) ) {
                    v = (inc < inlen)? (unsigned char)instr[inc] : 0;
                    inc++;

                    v = (unsigned char) ((v < 43 || v > 122) ? 0 : cd64[ v - 43 ]);
                    if( v ) {
                        v = (unsigned char) ((v == '$') ? 0 : v - 61);
                    }
                }
                if( inc<=inlen ) {
                    len++;
                }
            }
            if( len>1 ) {
                outc += (len-1);
            }
        }
        return outc;
    }

    while( inc < inlen ) {
        for( len = 0, i = 0; i < 4 && inc<inlen; i++ ) {
            v = 0;
            while( (inc<=inlen) && (v==0) ) {
                v = (inc < inlen)? (unsigned char)instr[inc] : 0;
                inc++;

                v = (unsigned char) ((v < 43 || v > 122) ? 0 : cd64[ v - 43 ]);
                if( v ) {
                    v = (unsigned char) ((v == '$') ? 0 : v - 61);
                }
            }
            if( inc<=inlen ) {
                len++;
                if( v ) {
                    in[ i ] = (unsigned char) (v - 1);
                }
            }
            else {
                in[i] = 0;
            }
        }
        if( len ) {
            decodeblock( in, out );
            for( i = 0; i < len - 1; i++ ) {
                outstr[outc++] = out[i];
            }
        }
    }
    return outc;
}

/*
** returnable errors
**
** Error codes returned to the operating system.
**
*/
#define B64_SYNTAX_ERROR        1
#define B64_FILE_ERROR          2
#define B64_FILE_IO_ERROR       3
#define B64_ERROR_OUT_CLOSE     4
#define B64_LINE_SIZE_TO_MIN    5

/*
** b64_message
**
** Gather text messages in one place.
**
*/
__attribute__((unused)) static char * b64_message( int errcode )
{
#define B64_MAX_MESSAGES 6

    static char * msgs[ B64_MAX_MESSAGES ] = {
            "b64:000:Invalid Message Code.",
            "b64:001:Syntax Error -- check help for usage.",
            "b64:002:File Error Opening/Creating Files.",
            "b64:003:File I/O Error -- Note: output file not removed.",
            "b64:004:Error on output file close.",
            "b64:004:linesize set to minimum."
    };

    char * msg = msgs[ 0 ];

    if( errcode > 0 && errcode < B64_MAX_MESSAGES ) {
        msg = msgs[ errcode ];
    }

    return( msg );
}

#if defined(__cplusplus)
}
#endif

#endif /* _B64_H_INCLUDED */
