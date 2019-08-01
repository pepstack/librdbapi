/***********************************************************************
* COPYRIGHT (C) 2018 PEPSTACK, PEPSTACK.COM
*
* THIS SOFTWARE IS PROVIDED 'AS-IS', WITHOUT ANY EXPRESS OR IMPLIED
* WARRANTY. IN NO EVENT WILL THE AUTHORS BE HELD LIABLE FOR ANY DAMAGES
* ARISING FROM THE USE OF THIS SOFTWARE.
*
* PERMISSION IS GRANTED TO ANYONE TO USE THIS SOFTWARE FOR ANY PURPOSE,
* INCLUDING COMMERCIAL APPLICATIONS, AND TO ALTER IT AND REDISTRIBUTE IT
* FREELY, SUBJECT TO THE FOLLOWING RESTRICTIONS:
*
*  THE ORIGIN OF THIS SOFTWARE MUST NOT BE MISREPRESENTED; YOU MUST NOT
*  CLAIM THAT YOU WROTE THE ORIGINAL SOFTWARE. IF YOU USE THIS SOFTWARE
*  IN A PRODUCT, AN ACKNOWLEDGMENT IN THE PRODUCT DOCUMENTATION WOULD
*  BE APPRECIATED BUT IS NOT REQUIRED.
*
*  ALTERED SOURCE VERSIONS MUST BE PLAINLY MARKED AS SUCH, AND MUST NOT
*  BE MISREPRESENTED AS BEING THE ORIGINAL SOFTWARE.
*
*  THIS NOTICE MAY NOT BE REMOVED OR ALTERED FROM ANY SOURCE DISTRIBUTION.
***********************************************************************/

/**
 * @file: bo.h
 *   Byte Converter for 2, 4, 8 bytes numeric types
 *
 * @author: master@pepstack.com
 *
 * @version: 1.8.8
 *
 * @create: 2013-06-19
 *
 * @update: 2019-01-21 15:28:28
 *
 *---------------------------------------------------------------------
 *  Big Endian: XDR (big endian) encoding of numeric types
 *
 *                   Register
 *                  0x0A0B0C0D
 *      Memory         | | | |
 *       |..|          | | | |
 *  a+0: |0A|<---------+ | | |
 *  a+1: |0B|<-----------+ | |
 *  a+2: |0C|<-------------+ |
 *  a+3: |0D|<---------------+
 *       |..|
 *
 *---------------------------------------------------------------------
 *  Little Endian: NDR (little endian) encoding of numeric types
 *
 *   Register
 *  0x0A0B0C0D
 *     | | | |              Memory
 *     | | | |               |..|
 *     | | | +--------> a+0: |0D|
 *     | | +----------> a+1: |0C|
 *     | +------------> a+2: |0B|
 *     +--------------> a+3: |0A|
 *                           |..|
 *---------------------------------------------------------------------
 */
#ifndef BYTE_ORDER_H_INCLUDED
#define BYTE_ORDER_H_INCLUDED

#if defined(__cplusplus)
extern "C"
{
#endif

#include <memory.h>


#ifdef _MSC_VER
    #define __INLINE static __forceinline
    #define __INLINE_ALL __INLINE
#else
    #define __INLINE static inline
    #if (defined(__APPLE__) && defined(__ppc__))
        /* static inline __attribute__ here breaks osx ppc gcc42 build */
        #define __INLINE_ALL static __attribute__((always_inline))
    #else
        #define __INLINE_ALL static inline __attribute__((always_inline))
    #endif
#endif

#if defined (_SVR4) || defined (SVR4) || defined (__OpenBSD__) ||\
    defined (_sgi) || defined (__sun) || defined (sun) || \
    defined (__digital__) || defined (__HP_cc)
    #include <inttypes.h>
#elif defined (_MSC_VER) && _MSC_VER < 1600
    /* VS 2010 (_MSC_VER 1600) has stdint.h */
    typedef __int8 int8_t;
    typedef unsigned __int8 uint8_t;
    typedef __int16 int16_t;
    typedef unsigned __int16 uint16_t;
    typedef __int32 int32_t;
    typedef unsigned __int32 uint32_t;
    typedef __int64 int64_t;
    typedef unsigned __int64 uint64_t;
#elif defined (_AIX)
    #include <sys/inttypes.h>
#else
    #include <stdint.h>
#endif

#ifndef byte_t_type_has_defined
    typedef unsigned char byte_t;
    #define byte_t_type_has_defined
#endif

static union {
    char c[4];
    uint8_t f;
} __endianess = {{'l','0','0','b'}};

#define __little_endian (((char)__endianess.f) == 'l')
#define __big_endian (((char)__endianess.f) == 'b')


/**
 * BO: Bit Op
 *
 * Setting a bit
 *   Use the bitwise OR operator (|) to set a bit.
 */
#define BO_set_bit(number, x) \
    (number) |= 1 << (x)

/**
 * Clearing a bit
 *   Use the bitwise AND operator (&) to clear a bit.
 *   That will clear bit x.You must invert the bit string with the bitwise NOT operator (~), then AND it.
 */
#define BO_clear_bit(number, x) \
    (number) &= ~(1 << (x))

/**
 * Toggling a bit
 *   The XOR operator (^) can be used to toggle a bit.
 */
#define BO_toggle_bit(number, x) \
    (number) ^= 1 << (x)

/**
 * Checking a bit
 *   To check a bit, shift the number x to the right, then bitwise AND it.
 */
#define BO_check_bit(number, x) \
    (((number) >> (x)) & 1)

/**
 * Changing the nth bit to x
 *   To check a bit, shift the number x to the right, then bitwise AND it.
 *   Setting the nth bit to either 1 or 0 can be achieved with the following
 *   Bit n will be set if x is 1, and cleared if x is 0.
 */
#define BO_change_bit(number, n, x) \
    (number) ^= (-(x) ^ (number)) & (1 << (n))


__INLINE void BO_swap_bytes(void *value, int size)
{
    int i = 0;
    uint8_t t;
    uint8_t *b = (uint8_t*) value;

    for (; i < size/2; ++i) {
        t = b[i];
        b[i] = b[size-i-1];
        b[size-i-1] = t;
    }
}


/**
 * 2 bytes numeric converter: int16_t, uint16_t
 */
__INLINE int16_t BO_i16_htole (int16_t val)
{
    if (__big_endian) {
        BO_swap_bytes(&val, sizeof(val));
    }
    return val;
}

__INLINE int16_t BO_i16_htobe (int16_t val)
{
    if (__little_endian) {
        BO_swap_bytes(&val, sizeof(val));
    }
    return val;
}

__INLINE int16_t BO_i16_letoh (int16_t val)
{
    if (__big_endian) {
        BO_swap_bytes(&val, sizeof(val));
    }
    return val;
}

__INLINE int16_t BO_i16_betoh (int16_t val)
{
    if (__little_endian) {
        BO_swap_bytes(&val, sizeof(val));
    }
    return val;
}

/**
 * 4 bytes numeric converter:
 *  int32_t, uint32_t
 *  float
 */
__INLINE int32_t BO_i32_htole (int32_t val)
{
    if (__big_endian) {
        BO_swap_bytes(&val, sizeof(val));
    }
    return val;
}

__INLINE int32_t BO_i32_htobe (int32_t val)
{
    if (__little_endian) {
        BO_swap_bytes(&val, sizeof(val));
    }
    return val;
}

__INLINE int32_t BO_i32_letoh (int32_t val)
{
    if (__big_endian) {
        BO_swap_bytes(&val, sizeof(val));
    }
    return val;
}

__INLINE int32_t BO_i32_betoh (int32_t val)
{
    if (__little_endian) {
        BO_swap_bytes(&val, sizeof(val));
    }
    return val;
}

__INLINE float BO_f32_htole (float val)
{
    if (__big_endian) {
        BO_swap_bytes(&val, sizeof(val));
    }
    return val;
}

__INLINE float BO_f32_htobe (float val)
{
    if (__little_endian) {
        BO_swap_bytes(&val, sizeof(val));
    }
    return val;
}

__INLINE float BO_f32_letoh (float val)
{
    if (__big_endian) {
        BO_swap_bytes(&val, sizeof(val));
    }
    return val;
}

__INLINE float BO_f32_betoh (float val)
{
    if (__little_endian) {
        BO_swap_bytes(&val, sizeof(val));
    }
    return val;
}

/**
 * 8 bytes numeric converter
 */
__INLINE double BO_f64_htole (double val)
{
    if (__big_endian) {
        BO_swap_bytes(&val, sizeof(val));
    }
    return val;
}

__INLINE double BO_f64_htobe (double val)
{
    if (__little_endian) {
        BO_swap_bytes(&val, sizeof(val));
    }
    return val;
}

__INLINE double BO_f64_letoh (double val)
{
    if (__big_endian) {
        BO_swap_bytes(&val, sizeof(val));
    }
    return val;
}

__INLINE double BO_f64_betoh (double val)
{
    if (__little_endian) {
        BO_swap_bytes(&val, sizeof(val));
    }
    return val;
}

__INLINE int64_t BO_i64_htole (int64_t val)
{
    if (__big_endian) {
        BO_swap_bytes(&val, sizeof(val));
    }
    return val;
}

__INLINE int64_t BO_i64_htobe (int64_t val)
{
    if (__little_endian) {
        BO_swap_bytes(&val, sizeof(val));
    }
    return val;
}

__INLINE int64_t BO_i64_letoh (int64_t val)
{
    if (__big_endian) {
        BO_swap_bytes(&val, sizeof(val));
    }
    return val;
}

__INLINE int64_t BO_i64_betoh (int64_t val)
{
    if (__little_endian) {
        BO_swap_bytes(&val, sizeof(val));
    }
    return val;
}

/**
 * read bytes to host
 */
__INLINE int16_t BO_bytes_betoh_i16 (void *b2)
{
    int16_t val = *(int16_t*) b2;
    if (__little_endian) {
        val = BO_i16_betoh (val);
    }
    return val;
}

__INLINE int16_t BO_bytes_letoh_i16 (void *b2)
{
    int16_t val = *(int16_t*) b2;
    if (__big_endian) {
        val = BO_i16_betoh (val);
    }
    return val;
}

__INLINE int32_t BO_bytes_betoh_i32 (void *b4)
{
    int32_t val = *(int32_t*) b4;
    if (__little_endian) {
        val = BO_i32_betoh (val);
    }
    return val;
}

__INLINE int32_t BO_bytes_letoh_i32 (void *b4)
{
    int32_t val = *(int32_t*) b4;
    if (__big_endian) {
        val = BO_i32_betoh (val);
    }
    return val;
}

__INLINE float BO_bytes_betoh_f32 (void *b4)
{
    float val = *(float*) b4;
    if (__little_endian) {
        val = BO_f32_betoh (val);
    }
    return val;
}

__INLINE float BO_bytes_letoh_f32 (void *b4)
{
    float val = *(float*) b4;
    if (__big_endian) {
        val = BO_f32_betoh (val);
    }
    return val;
}

__INLINE int64_t BO_bytes_betoh_i64 (void *b8)
{
    int64_t val = *(int64_t*) b8;
    if (__little_endian) {
        val = BO_i64_betoh (val);
    }
    return val;
}

__INLINE int64_t BO_bytes_letoh_i64 (void *b8)
{
    int64_t val = *(int64_t*) b8;
    if (__big_endian) {
        val = BO_i64_betoh (val);
    }
    return val;
}

__INLINE double BO_bytes_betoh_f64 (void *b8)
{
    double val = *(double*) b8;
    if (__little_endian) {
        val = BO_f64_betoh (val);
    }
    return val;
}

__INLINE double BO_bytes_letoh_f64 (void *b8)
{
    double val = *(double*) b8;
    if (__big_endian) {
        val = BO_f64_betoh (val);
    }
    return val;
}

#ifdef BYTEORDER_TEST
#include <assert.h>
#include <string.h>

static void byteorder_test_int16 (int16_t val)
{
    int16_t a, b;
    char b2[2];

    a = b = val;

    BO_swap_bytes(&a, sizeof(a));
    BO_swap_bytes(&a, sizeof(a));
    assert(a == b);

    b = BO_i16_htole(a);
    b = BO_i16_letoh(b);
    assert(a == b);

    b = BO_i16_htobe(a);
    b = BO_i16_betoh(b);
    assert(a == b);

    /* write as big endian to bytes */
    b = BO_i16_htobe(a);
    memcpy(b2, &b, sizeof(b));

    /* read bytes */
    b = * (int16_t*) b2;
    b = BO_i16_betoh(b);
    assert(a == b);

    b = BO_bytes_betoh_i16 (b2);
    assert(a == b);
}

static void byteorder_test_int32 (int val)
{
    int a, b;
    char b4[4];

    a = b = val;

    BO_swap_bytes(&a, sizeof(a));
    BO_swap_bytes(&a, sizeof(a));
    assert(a == b);

    b = BO_i32_htole(a);
    b = BO_i32_letoh(b);
    assert(a == b);

    b = BO_i32_htobe(a);
    b = BO_i32_betoh(b);
    assert(a == b);

    /* read bytes */
    b = BO_i32_htobe(a);
    memcpy(b4, &b, sizeof(b));
    b = BO_bytes_betoh_i32 (b4);
    assert(a == b);
}

static void byteorder_test_f32 (float val)
{
    float a, b;
    char b4[4];

    a = b = val;

    BO_swap_bytes(&a, sizeof(a));
    BO_swap_bytes(&a, sizeof(a));
    assert(a == b);

    b = BO_f32_htole(a);
    b = BO_f32_letoh(b);
    assert(a == b);

    b = BO_f32_htobe(a);
    b = BO_f32_betoh(b);
    assert(a == b);

    b = BO_f32_htobe(a);
    memcpy(b4, &b, sizeof(b));
    b = BO_bytes_betoh_f32 (b4);
    assert(a == b);
}

static void byteorder_test_f64 (double val)
{
    double a, b;
    char b8[8];

    a = b = val;

    BO_swap_bytes(&a, sizeof(a));
    BO_swap_bytes(&a, sizeof(a));
    assert(a == b);

    b = BO_f64_htole(a);
    b = BO_f64_letoh(b);
    assert(a == b);

    b = BO_f64_htobe(a);
    b = BO_f64_betoh(b);
    assert(a == b);

    b = BO_f64_htobe(a);
    memcpy(b8, &b, sizeof(b));
    b = BO_bytes_betoh_f64 (b8);
    assert(a == b);
}

#endif

#if defined(__cplusplus)
}
#endif

#endif /* BYTE_ORDER_H_INCLUDED */
