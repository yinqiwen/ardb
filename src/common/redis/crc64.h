#ifndef CRC64_H
#define CRC64_H
#ifdef __cplusplus
extern "C"
{
#endif
#include <stdint.h>

    uint64_t crc64(uint64_t crc, const unsigned char *s, uint64_t l);
#ifdef __cplusplus
}
#endif
#endif
