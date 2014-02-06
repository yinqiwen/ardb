/*
The MIT License

Copyright (c) 2011 lyo.kato@gmail.com

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

#include <ctype.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdbool.h>
#include <assert.h>

#include <geohash/geohash.h>

#define MAX_HASH_LENGTH 22

#define REFINE_RANGE(range, bits, offset) \
    if (((bits) & (offset)) == (offset)) \
        (range)->min = ((range)->max + (range)->min) / 2.0; \
    else \
        (range)->max = ((range)->max + (range)->min) / 2.0;

#define SET_BIT(bits, mid, range, value, offset) \
    mid = ((range)->max + (range)->min) / 2.0; \
    if ((value) >= mid) { \
        (range)->min = mid; \
        (bits) |= (0x1 << (offset)); \
    } else { \
        (range)->max = mid; \
        (bits) |= (0x0 << (offset)); \
    }

static const char BASE32_ENCODE_TABLE[33] = "0123456789bcdefghjkmnpqrstuvwxyz";
static const char BASE32_DECODE_TABLE[44] = {
    /* 0 */   0, /* 1 */   1, /* 2 */   2, /* 3 */   3, /* 4 */   4,
    /* 5 */   5, /* 6 */   6, /* 7 */   7, /* 8 */   8, /* 9 */   9,
    /* : */  -1, /* ; */  -1, /* < */  -1, /* = */  -1, /* > */  -1,
    /* ? */  -1, /* @ */  -1, /* A */  -1, /* B */  10, /* C */  11,
    /* D */  12, /* E */  13, /* F */  14, /* G */  15, /* H */  16,
    /* I */  -1, /* J */  17, /* K */  18, /* L */  -1, /* M */  19,
    /* N */  20, /* O */  -1, /* P */  21, /* Q */  22, /* R */  23,
    /* S */  24, /* T */  25, /* U */  26, /* V */  27, /* W */  28,
    /* X */  29, /* Y */  30, /* Z */  31
};

static const char NEIGHBORS_TABLE[8][33] = {
    "p0r21436x8zb9dcf5h7kjnmqesgutwvy", /* NORTH EVEN */
    "bc01fg45238967deuvhjyznpkmstqrwx", /* NORTH ODD  */
    "bc01fg45238967deuvhjyznpkmstqrwx", /* EAST EVEN  */
    "p0r21436x8zb9dcf5h7kjnmqesgutwvy", /* EAST ODD   */
    "238967debc01fg45kmstqrwxuvhjyznp", /* WEST EVEN  */
    "14365h7k9dcfesgujnmqp0r2twvyx8zb", /* WEST ODD   */
    "14365h7k9dcfesgujnmqp0r2twvyx8zb", /* SOUTH EVEN */
    "238967debc01fg45kmstqrwxuvhjyznp"  /* SOUTH ODD  */
};

static const char BORDERS_TABLE[8][9] = {
    "prxz",     /* NORTH EVEN */
    "bcfguvyz", /* NORTH ODD */
    "bcfguvyz", /* EAST  EVEN */
    "prxz",     /* EAST  ODD */
    "0145hjnp", /* WEST  EVEN */
    "028b",     /* WEST  ODD */
    "028b",     /* SOUTH EVEN */
    "0145hjnp"  /* SOUTH ODD */
};

bool
GEOHASH_verify_hash(const char *hash)
{
    const char *p;
    unsigned char c;
    p = hash;
    while (*p != '\0') {
        c = toupper(*p++);
        if (c < 0x30)
            return false;
        c -= 0x30;
        if (c > 43)
            return false;
        if (BASE32_DECODE_TABLE[c] == -1)
            return false;
    }
    return true;
}

GEOHASH_area*
GEOHASH_decode(const char *hash)
{
    const char *p;
    unsigned char c;
    char bits;
    GEOHASH_area *area;
    GEOHASH_range *range1, *range2, *range_tmp;

    area = (GEOHASH_area *)malloc(sizeof(GEOHASH_area));
    if (area == NULL)
        return NULL;

    area->latitude.max   =   90;
    area->latitude.min   =  -90;
    area->longitude.max =  180;
    area->longitude.min = -180;

    range1 = &area->longitude;
    range2 = &area->latitude;

    p = hash;

    while (*p != '\0') {

        c = toupper(*p++);
        if (c < 0x30) {
            free(area);
            return NULL;
        }
        c -= 0x30;
        if (c > 43) {
            free(area);
            return NULL;
        }
        bits = BASE32_DECODE_TABLE[c];
        if (bits == -1) {
            free(area);
            return NULL;
        }

        REFINE_RANGE(range1, bits, 0x10);
        REFINE_RANGE(range2, bits, 0x08);
        REFINE_RANGE(range1, bits, 0x04);
        REFINE_RANGE(range2, bits, 0x02);
        REFINE_RANGE(range1, bits, 0x01);

        range_tmp = range1;
        range1    = range2;
        range2    = range_tmp;
    }
    return area;
}

char*
GEOHASH_encode(double lat, double lon, unsigned int len)
{
    unsigned int i;
    char *hash;
    unsigned char bits = 0;
    double mid;
    GEOHASH_range lat_range = {  90,  -90 };
    GEOHASH_range lon_range = { 180, -180 };

    double val1, val2, val_tmp;
    GEOHASH_range *range1, *range2, *range_tmp;

    assert(lat >= -90.0);
    assert(lat <= 90.0);
    assert(lon >= -180.0);
    assert(lon <= 180.0);
    assert(len <= MAX_HASH_LENGTH);

    hash = (char *)malloc(sizeof(char) * (len + 1));
    if (hash == NULL)
        return NULL;

    val1 = lon; range1 = &lon_range;
    val2 = lat; range2 = &lat_range;

    for (i=0; i < len; i++) {

        bits = 0;
        SET_BIT(bits, mid, range1, val1, 4);
        SET_BIT(bits, mid, range2, val2, 3);
        SET_BIT(bits, mid, range1, val1, 2);
        SET_BIT(bits, mid, range2, val2, 1);
        SET_BIT(bits, mid, range1, val1, 0);

        hash[i] = BASE32_ENCODE_TABLE[bits];

        val_tmp   = val1;
        val1      = val2;
        val2      = val_tmp;
        range_tmp = range1;
        range1    = range2;
        range2    = range_tmp;
    }

    hash[len] = '\0';
    return hash;
}

void
GEOHASH_free_area(GEOHASH_area *area)
{
    free(area);
}

GEOHASH_neighbors*
GEOHASH_get_neighbors(const char* hash)
{
    GEOHASH_neighbors *neighbors;

    neighbors = (GEOHASH_neighbors*)malloc(sizeof(GEOHASH_neighbors));
    if (neighbors == NULL)
        return NULL;

    neighbors->north = GEOHASH_get_adjacent(hash, GEOHASH_NORTH);
    neighbors->east  = GEOHASH_get_adjacent(hash, GEOHASH_EAST);
    neighbors->west  = GEOHASH_get_adjacent(hash, GEOHASH_WEST);
    neighbors->south = GEOHASH_get_adjacent(hash, GEOHASH_SOUTH);

    neighbors->north_east = GEOHASH_get_adjacent(neighbors->north, GEOHASH_EAST);
    neighbors->north_west = GEOHASH_get_adjacent(neighbors->north, GEOHASH_WEST);
    neighbors->south_east = GEOHASH_get_adjacent(neighbors->south, GEOHASH_EAST);
    neighbors->south_west = GEOHASH_get_adjacent(neighbors->south, GEOHASH_WEST);

    return neighbors;
}

char*
GEOHASH_get_adjacent(const char* hash, GEOHASH_direction dir)
{
    int len, idx;
    const char *border_table, *neighbor_table;
    char *base, *refined_base, *ptr, last;

    assert(hash != NULL);

    len  = strlen(hash);
    last = tolower(hash[len - 1]);
    idx  = dir * 2 + (len % 2);

    border_table = BORDERS_TABLE[idx];

    base = (char *)malloc(sizeof(char) * (len + 1));
    if (base == NULL)
        return NULL;
    memset(base, '\0', sizeof(char) * (len + 1));

    strncpy(base, hash, len - 1);

    if (strchr(border_table, last) != NULL) {
        refined_base = GEOHASH_get_adjacent(base, dir);
        if (refined_base == NULL) {
            free(base);
            return NULL;
        }
        strncpy(base, refined_base, strlen(refined_base));
        free(refined_base);
    }

    neighbor_table = NEIGHBORS_TABLE[idx];

    ptr = strchr(neighbor_table, last);
    if (ptr == NULL) {
        free(base);
        return NULL;
    }
    idx = (int)(ptr - neighbor_table);
    len = strlen(base);
    base[len] = BASE32_ENCODE_TABLE[idx];
    return base;
}

void
GEOHASH_free_neighbors(GEOHASH_neighbors *neighbors)
{
    free(neighbors->north);
    free(neighbors->east);
    free(neighbors->west);
    free(neighbors->south);
    free(neighbors->north_east);
    free(neighbors->south_east);
    free(neighbors->north_west);
    free(neighbors->south_west);
    free(neighbors);
}

