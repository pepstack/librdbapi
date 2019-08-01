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
 *  rc4.h
 *
 *    rc4 encrypt and decrypt
 */
#ifndef RC4_H_INCLUDED
#define RC4_H_INCLUDED

#if defined(__cplusplus)
extern "C"
{
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>


#ifndef RC4_DEFAULT_KEY
#  define RC4_DEFAULT_KEY    "350137278@qq.com"
#endif


static unsigned char __rc4_sbox__[] = {
      0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,  15,
     16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,
     32,  33,  34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,
     48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,
     64,  65,  66,  67,  68,  69,  70,  71,  72,  73,  74,  75,  76,  77,  78,  79,
     80,  81,  82,  83,  84,  85,  86,  87,  88,  89,  90,  91,  92,  93,  94,  95,
     96,  97,  98,  99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111,
    112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127,
    128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143,
    144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159,
    160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175,
    176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191,
    192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207,
    208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223,
    224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239,
    240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255,
    0
};


static int rc4_encrypt (char *source, size_t sourcelen, char* key, size_t keylen)
{
    /* we will consider size of sbox 256 bytes
     *   (extra byte are only to prevent any mishep just in case)
     */
    char Sbox[257], Sbox2[257];

    unsigned long i, j, t, x;

    /* this unsecured key is to be used only when there is no source key from user */
    static const char OurUnSecuredKey[] = RC4_DEFAULT_KEY;

    size_t OurKeyLen = strlen( OurUnSecuredKey );

    char temp , k = 0;

    i = j = t = x = 0;
    temp = 0;

    /* always initialize the arrays with zero */
	memset(Sbox2, 0, sizeof(Sbox2));

    /* initialize sbox i */
    memcpy(Sbox, __rc4_sbox__, sizeof(Sbox));

    /* initialize sbox i
    for( i = 0; i < 256U; i++ ) {
        Sbox[i] = ( char ) i;
    }
    */

    j = 0;

    /* whether user has sent any sourceur key */
    if (keylen) {
        /* initialize the sbox2 with user key */
        for (i = 0; i < 256U ; i++ ) {
            if (j == keylen) {
                j = 0;
            }
            Sbox2[i] = key[j++];
        }
    } else {
        /* initialize the sbox2 with our key */
        for (i = 0; i < 256U ; i++) {
            if (j == OurKeyLen) {
                j = 0;
            }
            Sbox2[i] = OurUnSecuredKey[j++];
        }
    }

    j = 0 ; /* Initialize j */
    
	/* scramble sbox1 with sbox2 */
    for( i = 0; i < 256; i++ ) {
        j = ( j + ( unsigned long ) Sbox[i] + ( unsigned long ) Sbox2[i] ) % 256U ;
        temp =  Sbox[i];                    
        Sbox[i] = Sbox[j];
        Sbox[j] =  temp;
    }

    i = j = 0;

    for ( x = 0; x < sourcelen; x++ ) {
        /* increment i */
        i = (i + 1U) % 256U;
        
		/*increment j */
        j = (j + ( unsigned long ) Sbox[i] ) % 256U;

        /* Scramble SBox #1 further so encryption routine will
           will repeat itself at great interval */
        temp = Sbox[i];
        Sbox[i] = Sbox[j] ;
        Sbox[j] = temp;

        /* Get ready to create pseudo random  byte for encryption key */
        t = ( ( unsigned long ) Sbox[i] + ( unsigned long ) Sbox[j] ) %  256U ;

        /* get the random byte */
        k = Sbox[t];

        /* xor with the data and done */
        source[x] = ( source[x] ^ k );
    }

	return (int) sourcelen;
}

static int rc4_encrypt_file ( char *source, char* dest, char* key, size_t keylen, char *buffer, size_t buffer_size )
{
	size_t totalWrite = 0;
	size_t totalRead = 0;
    size_t sizeRead = 1;

	/* Reading crypted file and decrypt content */
	if (source == dest) {
		FILE  *fp = fopen(source, "rb+");
		if (! fp)
			return 0;

		while (sizeRead){
			if (0 != fseek(fp, (long)totalRead, SEEK_SET)){
				fclose(fp);
				return 0;
			}

			sizeRead = fread( buffer, sizeof(char), buffer_size, fp );
			if (sizeRead==0)
				break;
			totalRead += sizeRead;

			rc4_encrypt(buffer, sizeRead, key, keylen);
			
			if (0 != fseek(fp, (long)totalWrite, SEEK_SET)){
				fclose(fp);
				return 0;
			}

			totalWrite += fwrite(buffer, sizeof(char), sizeRead, fp);
		}
		fclose(fp);
	} else {
		FILE *fpr = 0;
		FILE *fpw = 0;

		fpr = fopen( source, "rb" );
		if (!fpr)
			return 0;

		fpw = fopen( dest, "wb" );
		if (!fpw){
			fclose(fpr);
			return 0;
		}
		
		while (!feof(fpr)) {
			sizeRead = fread( buffer, sizeof(char), buffer_size, fpr );
			rc4_encrypt(buffer, sizeRead, key, keylen);
			totalRead += sizeRead;
			totalWrite += fwrite(buffer, sizeof(char), sizeRead, fpw);
		}
		fclose(fpr);
		fclose(fpw);
	}

	if (totalRead==totalWrite) {
		return (int) totalWrite;
    }
	return 0;
}


/* Define the function ByteToStr. */
static void rc4_bytes2string(const unsigned char* pv, int cb, char *sz)
{
	unsigned char* pb = (unsigned char*) pv; /* Local pointer to a BYTE in the BYTE array */
	int i;                  /* Local loop counter */
	int b;                  /* Local variable */

	/*  Begin processing loop. */
	for (i = 0; i<cb; i++) {
	   b = (*pb & 0xF0) >> 4;
	   *sz++ = (b <= 9) ? b + '0' : (b - 10) + 'A';
	   b = *pb & 0x0F;
	   *sz++ = (b <= 9) ? b + '0' : (b - 10) + 'A';
	   pb++;
	}
	*sz++ = 0;
}


static int rc4_hexencode(const unsigned char *source, size_t sourceSize, char* dest, size_t destSize)
{
	rc4_bytes2string(source, (int)sourceSize, dest);

	return (int)(sourceSize*2);
}


static int rc4_hexdecode(const char *source, size_t sourcelen, char* dest, size_t destSize)
{
	size_t  i;
	char    tmp[3];

	*dest = 0;

	tmp[2] = 0;
	destSize = 0;

	for (i = 0 ; i < sourcelen ; i++) {
		if (i%2 == 0) {
			tmp[0] = source[i];
			tmp[1] = source[i+1];
			dest[destSize++] = (unsigned char) strtol(tmp, 0, 16);
		}
	}

	return (int) destSize;
}

#if defined(__cplusplus)
}
#endif

#endif /* RC4_H_INCLUDED */
