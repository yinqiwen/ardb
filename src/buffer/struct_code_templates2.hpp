#ifndef STRUCT_CODE_TEMPLATES2_HPP_
#define STRUCT_CODE_TEMPLATES2_HPP_

namespace ardb
{

    template<typename A0>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 1; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 2; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 3; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 4; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 5; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 6; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 7; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 8; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 9; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 10; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 11; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 12; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 13; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    case 12:
                        if (!decode_arg(buf, a12))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 14; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    case 12:
                        if (!decode_arg(buf, a12))
                            return false;
                        break;
                    case 13:
                        if (!decode_arg(buf, a13))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 15; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    case 12:
                        if (!decode_arg(buf, a12))
                            return false;
                        break;
                    case 13:
                        if (!decode_arg(buf, a13))
                            return false;
                        break;
                    case 14:
                        if (!decode_arg(buf, a14))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14, typename A15>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 16; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    case 12:
                        if (!decode_arg(buf, a12))
                            return false;
                        break;
                    case 13:
                        if (!decode_arg(buf, a13))
                            return false;
                        break;
                    case 14:
                        if (!decode_arg(buf, a14))
                            return false;
                        break;
                    case 15:
                        if (!decode_arg(buf, a15))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 17; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    case 12:
                        if (!decode_arg(buf, a12))
                            return false;
                        break;
                    case 13:
                        if (!decode_arg(buf, a13))
                            return false;
                        break;
                    case 14:
                        if (!decode_arg(buf, a14))
                            return false;
                        break;
                    case 15:
                        if (!decode_arg(buf, a15))
                            return false;
                        break;
                    case 16:
                        if (!decode_arg(buf, a16))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 18; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    case 12:
                        if (!decode_arg(buf, a12))
                            return false;
                        break;
                    case 13:
                        if (!decode_arg(buf, a13))
                            return false;
                        break;
                    case 14:
                        if (!decode_arg(buf, a14))
                            return false;
                        break;
                    case 15:
                        if (!decode_arg(buf, a15))
                            return false;
                        break;
                    case 16:
                        if (!decode_arg(buf, a16))
                            return false;
                        break;
                    case 17:
                        if (!decode_arg(buf, a17))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 19; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    case 12:
                        if (!decode_arg(buf, a12))
                            return false;
                        break;
                    case 13:
                        if (!decode_arg(buf, a13))
                            return false;
                        break;
                    case 14:
                        if (!decode_arg(buf, a14))
                            return false;
                        break;
                    case 15:
                        if (!decode_arg(buf, a15))
                            return false;
                        break;
                    case 16:
                        if (!decode_arg(buf, a16))
                            return false;
                        break;
                    case 17:
                        if (!decode_arg(buf, a17))
                            return false;
                        break;
                    case 18:
                        if (!decode_arg(buf, a18))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 20; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    case 12:
                        if (!decode_arg(buf, a12))
                            return false;
                        break;
                    case 13:
                        if (!decode_arg(buf, a13))
                            return false;
                        break;
                    case 14:
                        if (!decode_arg(buf, a14))
                            return false;
                        break;
                    case 15:
                        if (!decode_arg(buf, a15))
                            return false;
                        break;
                    case 16:
                        if (!decode_arg(buf, a16))
                            return false;
                        break;
                    case 17:
                        if (!decode_arg(buf, a17))
                            return false;
                        break;
                    case 18:
                        if (!decode_arg(buf, a18))
                            return false;
                        break;
                    case 19:
                        if (!decode_arg(buf, a19))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 21; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    case 12:
                        if (!decode_arg(buf, a12))
                            return false;
                        break;
                    case 13:
                        if (!decode_arg(buf, a13))
                            return false;
                        break;
                    case 14:
                        if (!decode_arg(buf, a14))
                            return false;
                        break;
                    case 15:
                        if (!decode_arg(buf, a15))
                            return false;
                        break;
                    case 16:
                        if (!decode_arg(buf, a16))
                            return false;
                        break;
                    case 17:
                        if (!decode_arg(buf, a17))
                            return false;
                        break;
                    case 18:
                        if (!decode_arg(buf, a18))
                            return false;
                        break;
                    case 19:
                        if (!decode_arg(buf, a19))
                            return false;
                        break;
                    case 20:
                        if (!decode_arg(buf, a20))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 22; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    case 12:
                        if (!decode_arg(buf, a12))
                            return false;
                        break;
                    case 13:
                        if (!decode_arg(buf, a13))
                            return false;
                        break;
                    case 14:
                        if (!decode_arg(buf, a14))
                            return false;
                        break;
                    case 15:
                        if (!decode_arg(buf, a15))
                            return false;
                        break;
                    case 16:
                        if (!decode_arg(buf, a16))
                            return false;
                        break;
                    case 17:
                        if (!decode_arg(buf, a17))
                            return false;
                        break;
                    case 18:
                        if (!decode_arg(buf, a18))
                            return false;
                        break;
                    case 19:
                        if (!decode_arg(buf, a19))
                            return false;
                        break;
                    case 20:
                        if (!decode_arg(buf, a20))
                            return false;
                        break;
                    case 21:
                        if (!decode_arg(buf, a21))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21,
            typename A22>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21, A22& a22)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 23; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    case 12:
                        if (!decode_arg(buf, a12))
                            return false;
                        break;
                    case 13:
                        if (!decode_arg(buf, a13))
                            return false;
                        break;
                    case 14:
                        if (!decode_arg(buf, a14))
                            return false;
                        break;
                    case 15:
                        if (!decode_arg(buf, a15))
                            return false;
                        break;
                    case 16:
                        if (!decode_arg(buf, a16))
                            return false;
                        break;
                    case 17:
                        if (!decode_arg(buf, a17))
                            return false;
                        break;
                    case 18:
                        if (!decode_arg(buf, a18))
                            return false;
                        break;
                    case 19:
                        if (!decode_arg(buf, a19))
                            return false;
                        break;
                    case 20:
                        if (!decode_arg(buf, a20))
                            return false;
                        break;
                    case 21:
                        if (!decode_arg(buf, a21))
                            return false;
                        break;
                    case 22:
                        if (!decode_arg(buf, a22))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21,
            typename A22, typename A23>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21, A22& a22, A23& a23)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 24; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    case 12:
                        if (!decode_arg(buf, a12))
                            return false;
                        break;
                    case 13:
                        if (!decode_arg(buf, a13))
                            return false;
                        break;
                    case 14:
                        if (!decode_arg(buf, a14))
                            return false;
                        break;
                    case 15:
                        if (!decode_arg(buf, a15))
                            return false;
                        break;
                    case 16:
                        if (!decode_arg(buf, a16))
                            return false;
                        break;
                    case 17:
                        if (!decode_arg(buf, a17))
                            return false;
                        break;
                    case 18:
                        if (!decode_arg(buf, a18))
                            return false;
                        break;
                    case 19:
                        if (!decode_arg(buf, a19))
                            return false;
                        break;
                    case 20:
                        if (!decode_arg(buf, a20))
                            return false;
                        break;
                    case 21:
                        if (!decode_arg(buf, a21))
                            return false;
                        break;
                    case 22:
                        if (!decode_arg(buf, a22))
                            return false;
                        break;
                    case 23:
                        if (!decode_arg(buf, a23))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21,
            typename A22, typename A23, typename A24>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21, A22& a22, A23& a23, A24& a24)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 25; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    case 12:
                        if (!decode_arg(buf, a12))
                            return false;
                        break;
                    case 13:
                        if (!decode_arg(buf, a13))
                            return false;
                        break;
                    case 14:
                        if (!decode_arg(buf, a14))
                            return false;
                        break;
                    case 15:
                        if (!decode_arg(buf, a15))
                            return false;
                        break;
                    case 16:
                        if (!decode_arg(buf, a16))
                            return false;
                        break;
                    case 17:
                        if (!decode_arg(buf, a17))
                            return false;
                        break;
                    case 18:
                        if (!decode_arg(buf, a18))
                            return false;
                        break;
                    case 19:
                        if (!decode_arg(buf, a19))
                            return false;
                        break;
                    case 20:
                        if (!decode_arg(buf, a20))
                            return false;
                        break;
                    case 21:
                        if (!decode_arg(buf, a21))
                            return false;
                        break;
                    case 22:
                        if (!decode_arg(buf, a22))
                            return false;
                        break;
                    case 23:
                        if (!decode_arg(buf, a23))
                            return false;
                        break;
                    case 24:
                        if (!decode_arg(buf, a24))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21,
            typename A22, typename A23, typename A24, typename A25>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21, A22& a22, A23& a23, A24& a24, A25& a25)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 26; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    case 12:
                        if (!decode_arg(buf, a12))
                            return false;
                        break;
                    case 13:
                        if (!decode_arg(buf, a13))
                            return false;
                        break;
                    case 14:
                        if (!decode_arg(buf, a14))
                            return false;
                        break;
                    case 15:
                        if (!decode_arg(buf, a15))
                            return false;
                        break;
                    case 16:
                        if (!decode_arg(buf, a16))
                            return false;
                        break;
                    case 17:
                        if (!decode_arg(buf, a17))
                            return false;
                        break;
                    case 18:
                        if (!decode_arg(buf, a18))
                            return false;
                        break;
                    case 19:
                        if (!decode_arg(buf, a19))
                            return false;
                        break;
                    case 20:
                        if (!decode_arg(buf, a20))
                            return false;
                        break;
                    case 21:
                        if (!decode_arg(buf, a21))
                            return false;
                        break;
                    case 22:
                        if (!decode_arg(buf, a22))
                            return false;
                        break;
                    case 23:
                        if (!decode_arg(buf, a23))
                            return false;
                        break;
                    case 24:
                        if (!decode_arg(buf, a24))
                            return false;
                        break;
                    case 25:
                        if (!decode_arg(buf, a25))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21,
            typename A22, typename A23, typename A24, typename A25, typename A26>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21, A22& a22, A23& a23, A24& a24, A25& a25, A26& a26)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 27; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    case 12:
                        if (!decode_arg(buf, a12))
                            return false;
                        break;
                    case 13:
                        if (!decode_arg(buf, a13))
                            return false;
                        break;
                    case 14:
                        if (!decode_arg(buf, a14))
                            return false;
                        break;
                    case 15:
                        if (!decode_arg(buf, a15))
                            return false;
                        break;
                    case 16:
                        if (!decode_arg(buf, a16))
                            return false;
                        break;
                    case 17:
                        if (!decode_arg(buf, a17))
                            return false;
                        break;
                    case 18:
                        if (!decode_arg(buf, a18))
                            return false;
                        break;
                    case 19:
                        if (!decode_arg(buf, a19))
                            return false;
                        break;
                    case 20:
                        if (!decode_arg(buf, a20))
                            return false;
                        break;
                    case 21:
                        if (!decode_arg(buf, a21))
                            return false;
                        break;
                    case 22:
                        if (!decode_arg(buf, a22))
                            return false;
                        break;
                    case 23:
                        if (!decode_arg(buf, a23))
                            return false;
                        break;
                    case 24:
                        if (!decode_arg(buf, a24))
                            return false;
                        break;
                    case 25:
                        if (!decode_arg(buf, a25))
                            return false;
                        break;
                    case 26:
                        if (!decode_arg(buf, a26))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21,
            typename A22, typename A23, typename A24, typename A25, typename A26, typename A27>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21, A22& a22, A23& a23, A24& a24, A25& a25, A26& a26, A27& a27)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 28; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    case 12:
                        if (!decode_arg(buf, a12))
                            return false;
                        break;
                    case 13:
                        if (!decode_arg(buf, a13))
                            return false;
                        break;
                    case 14:
                        if (!decode_arg(buf, a14))
                            return false;
                        break;
                    case 15:
                        if (!decode_arg(buf, a15))
                            return false;
                        break;
                    case 16:
                        if (!decode_arg(buf, a16))
                            return false;
                        break;
                    case 17:
                        if (!decode_arg(buf, a17))
                            return false;
                        break;
                    case 18:
                        if (!decode_arg(buf, a18))
                            return false;
                        break;
                    case 19:
                        if (!decode_arg(buf, a19))
                            return false;
                        break;
                    case 20:
                        if (!decode_arg(buf, a20))
                            return false;
                        break;
                    case 21:
                        if (!decode_arg(buf, a21))
                            return false;
                        break;
                    case 22:
                        if (!decode_arg(buf, a22))
                            return false;
                        break;
                    case 23:
                        if (!decode_arg(buf, a23))
                            return false;
                        break;
                    case 24:
                        if (!decode_arg(buf, a24))
                            return false;
                        break;
                    case 25:
                        if (!decode_arg(buf, a25))
                            return false;
                        break;
                    case 26:
                        if (!decode_arg(buf, a26))
                            return false;
                        break;
                    case 27:
                        if (!decode_arg(buf, a27))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21,
            typename A22, typename A23, typename A24, typename A25, typename A26, typename A27, typename A28>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21, A22& a22, A23& a23, A24& a24, A25& a25, A26& a26, A27& a27, A28& a28)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 29; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    case 12:
                        if (!decode_arg(buf, a12))
                            return false;
                        break;
                    case 13:
                        if (!decode_arg(buf, a13))
                            return false;
                        break;
                    case 14:
                        if (!decode_arg(buf, a14))
                            return false;
                        break;
                    case 15:
                        if (!decode_arg(buf, a15))
                            return false;
                        break;
                    case 16:
                        if (!decode_arg(buf, a16))
                            return false;
                        break;
                    case 17:
                        if (!decode_arg(buf, a17))
                            return false;
                        break;
                    case 18:
                        if (!decode_arg(buf, a18))
                            return false;
                        break;
                    case 19:
                        if (!decode_arg(buf, a19))
                            return false;
                        break;
                    case 20:
                        if (!decode_arg(buf, a20))
                            return false;
                        break;
                    case 21:
                        if (!decode_arg(buf, a21))
                            return false;
                        break;
                    case 22:
                        if (!decode_arg(buf, a22))
                            return false;
                        break;
                    case 23:
                        if (!decode_arg(buf, a23))
                            return false;
                        break;
                    case 24:
                        if (!decode_arg(buf, a24))
                            return false;
                        break;
                    case 25:
                        if (!decode_arg(buf, a25))
                            return false;
                        break;
                    case 26:
                        if (!decode_arg(buf, a26))
                            return false;
                        break;
                    case 27:
                        if (!decode_arg(buf, a27))
                            return false;
                        break;
                    case 28:
                        if (!decode_arg(buf, a28))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21,
            typename A22, typename A23, typename A24, typename A25, typename A26, typename A27, typename A28,
            typename A29>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21, A22& a22, A23& a23, A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 30; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    case 12:
                        if (!decode_arg(buf, a12))
                            return false;
                        break;
                    case 13:
                        if (!decode_arg(buf, a13))
                            return false;
                        break;
                    case 14:
                        if (!decode_arg(buf, a14))
                            return false;
                        break;
                    case 15:
                        if (!decode_arg(buf, a15))
                            return false;
                        break;
                    case 16:
                        if (!decode_arg(buf, a16))
                            return false;
                        break;
                    case 17:
                        if (!decode_arg(buf, a17))
                            return false;
                        break;
                    case 18:
                        if (!decode_arg(buf, a18))
                            return false;
                        break;
                    case 19:
                        if (!decode_arg(buf, a19))
                            return false;
                        break;
                    case 20:
                        if (!decode_arg(buf, a20))
                            return false;
                        break;
                    case 21:
                        if (!decode_arg(buf, a21))
                            return false;
                        break;
                    case 22:
                        if (!decode_arg(buf, a22))
                            return false;
                        break;
                    case 23:
                        if (!decode_arg(buf, a23))
                            return false;
                        break;
                    case 24:
                        if (!decode_arg(buf, a24))
                            return false;
                        break;
                    case 25:
                        if (!decode_arg(buf, a25))
                            return false;
                        break;
                    case 26:
                        if (!decode_arg(buf, a26))
                            return false;
                        break;
                    case 27:
                        if (!decode_arg(buf, a27))
                            return false;
                        break;
                    case 28:
                        if (!decode_arg(buf, a28))
                            return false;
                        break;
                    case 29:
                        if (!decode_arg(buf, a29))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21,
            typename A22, typename A23, typename A24, typename A25, typename A26, typename A27, typename A28,
            typename A29, typename A30>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21, A22& a22, A23& a23, A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
            A30& a30)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 31; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    case 12:
                        if (!decode_arg(buf, a12))
                            return false;
                        break;
                    case 13:
                        if (!decode_arg(buf, a13))
                            return false;
                        break;
                    case 14:
                        if (!decode_arg(buf, a14))
                            return false;
                        break;
                    case 15:
                        if (!decode_arg(buf, a15))
                            return false;
                        break;
                    case 16:
                        if (!decode_arg(buf, a16))
                            return false;
                        break;
                    case 17:
                        if (!decode_arg(buf, a17))
                            return false;
                        break;
                    case 18:
                        if (!decode_arg(buf, a18))
                            return false;
                        break;
                    case 19:
                        if (!decode_arg(buf, a19))
                            return false;
                        break;
                    case 20:
                        if (!decode_arg(buf, a20))
                            return false;
                        break;
                    case 21:
                        if (!decode_arg(buf, a21))
                            return false;
                        break;
                    case 22:
                        if (!decode_arg(buf, a22))
                            return false;
                        break;
                    case 23:
                        if (!decode_arg(buf, a23))
                            return false;
                        break;
                    case 24:
                        if (!decode_arg(buf, a24))
                            return false;
                        break;
                    case 25:
                        if (!decode_arg(buf, a25))
                            return false;
                        break;
                    case 26:
                        if (!decode_arg(buf, a26))
                            return false;
                        break;
                    case 27:
                        if (!decode_arg(buf, a27))
                            return false;
                        break;
                    case 28:
                        if (!decode_arg(buf, a28))
                            return false;
                        break;
                    case 29:
                        if (!decode_arg(buf, a29))
                            return false;
                        break;
                    case 30:
                        if (!decode_arg(buf, a30))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21,
            typename A22, typename A23, typename A24, typename A25, typename A26, typename A27, typename A28,
            typename A29, typename A30, typename A31>
    inline bool decode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21, A22& a22, A23& a23, A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
            A30& a30, A31& a31)
    {
        uint32_t size = 0;
        if (!BufferHelper::ReadFixUInt32(buf, size))
            return false;
        uint32_t cursor = buf.GetReadIndex();
        for (uint32_t i = 0; i < 32; i++)
        {
            if (BufferHelper::ReadVarUInt32IfEqual(buf, i))
            {
                switch (i)
                {
                    case 0:
                        if (!decode_arg(buf, a0))
                            return false;
                        break;
                    case 1:
                        if (!decode_arg(buf, a1))
                            return false;
                        break;
                    case 2:
                        if (!decode_arg(buf, a2))
                            return false;
                        break;
                    case 3:
                        if (!decode_arg(buf, a3))
                            return false;
                        break;
                    case 4:
                        if (!decode_arg(buf, a4))
                            return false;
                        break;
                    case 5:
                        if (!decode_arg(buf, a5))
                            return false;
                        break;
                    case 6:
                        if (!decode_arg(buf, a6))
                            return false;
                        break;
                    case 7:
                        if (!decode_arg(buf, a7))
                            return false;
                        break;
                    case 8:
                        if (!decode_arg(buf, a8))
                            return false;
                        break;
                    case 9:
                        if (!decode_arg(buf, a9))
                            return false;
                        break;
                    case 10:
                        if (!decode_arg(buf, a10))
                            return false;
                        break;
                    case 11:
                        if (!decode_arg(buf, a11))
                            return false;
                        break;
                    case 12:
                        if (!decode_arg(buf, a12))
                            return false;
                        break;
                    case 13:
                        if (!decode_arg(buf, a13))
                            return false;
                        break;
                    case 14:
                        if (!decode_arg(buf, a14))
                            return false;
                        break;
                    case 15:
                        if (!decode_arg(buf, a15))
                            return false;
                        break;
                    case 16:
                        if (!decode_arg(buf, a16))
                            return false;
                        break;
                    case 17:
                        if (!decode_arg(buf, a17))
                            return false;
                        break;
                    case 18:
                        if (!decode_arg(buf, a18))
                            return false;
                        break;
                    case 19:
                        if (!decode_arg(buf, a19))
                            return false;
                        break;
                    case 20:
                        if (!decode_arg(buf, a20))
                            return false;
                        break;
                    case 21:
                        if (!decode_arg(buf, a21))
                            return false;
                        break;
                    case 22:
                        if (!decode_arg(buf, a22))
                            return false;
                        break;
                    case 23:
                        if (!decode_arg(buf, a23))
                            return false;
                        break;
                    case 24:
                        if (!decode_arg(buf, a24))
                            return false;
                        break;
                    case 25:
                        if (!decode_arg(buf, a25))
                            return false;
                        break;
                    case 26:
                        if (!decode_arg(buf, a26))
                            return false;
                        break;
                    case 27:
                        if (!decode_arg(buf, a27))
                            return false;
                        break;
                    case 28:
                        if (!decode_arg(buf, a28))
                            return false;
                        break;
                    case 29:
                        if (!decode_arg(buf, a29))
                            return false;
                        break;
                    case 30:
                        if (!decode_arg(buf, a30))
                            return false;
                        break;
                    case 31:
                        if (!decode_arg(buf, a31))
                            return false;
                        break;
                    default:
                        return false;
                }
            }
        }
        uint32_t end = buf.GetReadIndex();
        if (end - cursor > size)
            buf.AdvanceReadIndex(cursor + size - end);
        return true;
    }

    template<typename A0>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 1; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 2; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 3; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 4; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 5; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 6; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 7; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 8; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 9; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 10; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 11; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 12; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 13; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
                case 12:
                    encode_arg(buf, a12);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 14; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
                case 12:
                    encode_arg(buf, a12);
                    break;
                case 13:
                    encode_arg(buf, a13);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 15; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
                case 12:
                    encode_arg(buf, a12);
                    break;
                case 13:
                    encode_arg(buf, a13);
                    break;
                case 14:
                    encode_arg(buf, a14);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14, typename A15>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 16; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
                case 12:
                    encode_arg(buf, a12);
                    break;
                case 13:
                    encode_arg(buf, a13);
                    break;
                case 14:
                    encode_arg(buf, a14);
                    break;
                case 15:
                    encode_arg(buf, a15);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 17; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
                case 12:
                    encode_arg(buf, a12);
                    break;
                case 13:
                    encode_arg(buf, a13);
                    break;
                case 14:
                    encode_arg(buf, a14);
                    break;
                case 15:
                    encode_arg(buf, a15);
                    break;
                case 16:
                    encode_arg(buf, a16);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 18; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
                case 12:
                    encode_arg(buf, a12);
                    break;
                case 13:
                    encode_arg(buf, a13);
                    break;
                case 14:
                    encode_arg(buf, a14);
                    break;
                case 15:
                    encode_arg(buf, a15);
                    break;
                case 16:
                    encode_arg(buf, a16);
                    break;
                case 17:
                    encode_arg(buf, a17);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 19; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
                case 12:
                    encode_arg(buf, a12);
                    break;
                case 13:
                    encode_arg(buf, a13);
                    break;
                case 14:
                    encode_arg(buf, a14);
                    break;
                case 15:
                    encode_arg(buf, a15);
                    break;
                case 16:
                    encode_arg(buf, a16);
                    break;
                case 17:
                    encode_arg(buf, a17);
                    break;
                case 18:
                    encode_arg(buf, a18);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 20; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
                case 12:
                    encode_arg(buf, a12);
                    break;
                case 13:
                    encode_arg(buf, a13);
                    break;
                case 14:
                    encode_arg(buf, a14);
                    break;
                case 15:
                    encode_arg(buf, a15);
                    break;
                case 16:
                    encode_arg(buf, a16);
                    break;
                case 17:
                    encode_arg(buf, a17);
                    break;
                case 18:
                    encode_arg(buf, a18);
                    break;
                case 19:
                    encode_arg(buf, a19);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 21; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
                case 12:
                    encode_arg(buf, a12);
                    break;
                case 13:
                    encode_arg(buf, a13);
                    break;
                case 14:
                    encode_arg(buf, a14);
                    break;
                case 15:
                    encode_arg(buf, a15);
                    break;
                case 16:
                    encode_arg(buf, a16);
                    break;
                case 17:
                    encode_arg(buf, a17);
                    break;
                case 18:
                    encode_arg(buf, a18);
                    break;
                case 19:
                    encode_arg(buf, a19);
                    break;
                case 20:
                    encode_arg(buf, a20);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 22; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
                case 12:
                    encode_arg(buf, a12);
                    break;
                case 13:
                    encode_arg(buf, a13);
                    break;
                case 14:
                    encode_arg(buf, a14);
                    break;
                case 15:
                    encode_arg(buf, a15);
                    break;
                case 16:
                    encode_arg(buf, a16);
                    break;
                case 17:
                    encode_arg(buf, a17);
                    break;
                case 18:
                    encode_arg(buf, a18);
                    break;
                case 19:
                    encode_arg(buf, a19);
                    break;
                case 20:
                    encode_arg(buf, a20);
                    break;
                case 21:
                    encode_arg(buf, a21);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21,
            typename A22>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21, A22& a22)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 23; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
                case 12:
                    encode_arg(buf, a12);
                    break;
                case 13:
                    encode_arg(buf, a13);
                    break;
                case 14:
                    encode_arg(buf, a14);
                    break;
                case 15:
                    encode_arg(buf, a15);
                    break;
                case 16:
                    encode_arg(buf, a16);
                    break;
                case 17:
                    encode_arg(buf, a17);
                    break;
                case 18:
                    encode_arg(buf, a18);
                    break;
                case 19:
                    encode_arg(buf, a19);
                    break;
                case 20:
                    encode_arg(buf, a20);
                    break;
                case 21:
                    encode_arg(buf, a21);
                    break;
                case 22:
                    encode_arg(buf, a22);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21,
            typename A22, typename A23>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21, A22& a22, A23& a23)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 24; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
                case 12:
                    encode_arg(buf, a12);
                    break;
                case 13:
                    encode_arg(buf, a13);
                    break;
                case 14:
                    encode_arg(buf, a14);
                    break;
                case 15:
                    encode_arg(buf, a15);
                    break;
                case 16:
                    encode_arg(buf, a16);
                    break;
                case 17:
                    encode_arg(buf, a17);
                    break;
                case 18:
                    encode_arg(buf, a18);
                    break;
                case 19:
                    encode_arg(buf, a19);
                    break;
                case 20:
                    encode_arg(buf, a20);
                    break;
                case 21:
                    encode_arg(buf, a21);
                    break;
                case 22:
                    encode_arg(buf, a22);
                    break;
                case 23:
                    encode_arg(buf, a23);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21,
            typename A22, typename A23, typename A24>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21, A22& a22, A23& a23, A24& a24)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 25; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
                case 12:
                    encode_arg(buf, a12);
                    break;
                case 13:
                    encode_arg(buf, a13);
                    break;
                case 14:
                    encode_arg(buf, a14);
                    break;
                case 15:
                    encode_arg(buf, a15);
                    break;
                case 16:
                    encode_arg(buf, a16);
                    break;
                case 17:
                    encode_arg(buf, a17);
                    break;
                case 18:
                    encode_arg(buf, a18);
                    break;
                case 19:
                    encode_arg(buf, a19);
                    break;
                case 20:
                    encode_arg(buf, a20);
                    break;
                case 21:
                    encode_arg(buf, a21);
                    break;
                case 22:
                    encode_arg(buf, a22);
                    break;
                case 23:
                    encode_arg(buf, a23);
                    break;
                case 24:
                    encode_arg(buf, a24);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21,
            typename A22, typename A23, typename A24, typename A25>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21, A22& a22, A23& a23, A24& a24, A25& a25)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 26; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
                case 12:
                    encode_arg(buf, a12);
                    break;
                case 13:
                    encode_arg(buf, a13);
                    break;
                case 14:
                    encode_arg(buf, a14);
                    break;
                case 15:
                    encode_arg(buf, a15);
                    break;
                case 16:
                    encode_arg(buf, a16);
                    break;
                case 17:
                    encode_arg(buf, a17);
                    break;
                case 18:
                    encode_arg(buf, a18);
                    break;
                case 19:
                    encode_arg(buf, a19);
                    break;
                case 20:
                    encode_arg(buf, a20);
                    break;
                case 21:
                    encode_arg(buf, a21);
                    break;
                case 22:
                    encode_arg(buf, a22);
                    break;
                case 23:
                    encode_arg(buf, a23);
                    break;
                case 24:
                    encode_arg(buf, a24);
                    break;
                case 25:
                    encode_arg(buf, a25);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21,
            typename A22, typename A23, typename A24, typename A25, typename A26>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21, A22& a22, A23& a23, A24& a24, A25& a25, A26& a26)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 27; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
                case 12:
                    encode_arg(buf, a12);
                    break;
                case 13:
                    encode_arg(buf, a13);
                    break;
                case 14:
                    encode_arg(buf, a14);
                    break;
                case 15:
                    encode_arg(buf, a15);
                    break;
                case 16:
                    encode_arg(buf, a16);
                    break;
                case 17:
                    encode_arg(buf, a17);
                    break;
                case 18:
                    encode_arg(buf, a18);
                    break;
                case 19:
                    encode_arg(buf, a19);
                    break;
                case 20:
                    encode_arg(buf, a20);
                    break;
                case 21:
                    encode_arg(buf, a21);
                    break;
                case 22:
                    encode_arg(buf, a22);
                    break;
                case 23:
                    encode_arg(buf, a23);
                    break;
                case 24:
                    encode_arg(buf, a24);
                    break;
                case 25:
                    encode_arg(buf, a25);
                    break;
                case 26:
                    encode_arg(buf, a26);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21,
            typename A22, typename A23, typename A24, typename A25, typename A26, typename A27>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21, A22& a22, A23& a23, A24& a24, A25& a25, A26& a26, A27& a27)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 28; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
                case 12:
                    encode_arg(buf, a12);
                    break;
                case 13:
                    encode_arg(buf, a13);
                    break;
                case 14:
                    encode_arg(buf, a14);
                    break;
                case 15:
                    encode_arg(buf, a15);
                    break;
                case 16:
                    encode_arg(buf, a16);
                    break;
                case 17:
                    encode_arg(buf, a17);
                    break;
                case 18:
                    encode_arg(buf, a18);
                    break;
                case 19:
                    encode_arg(buf, a19);
                    break;
                case 20:
                    encode_arg(buf, a20);
                    break;
                case 21:
                    encode_arg(buf, a21);
                    break;
                case 22:
                    encode_arg(buf, a22);
                    break;
                case 23:
                    encode_arg(buf, a23);
                    break;
                case 24:
                    encode_arg(buf, a24);
                    break;
                case 25:
                    encode_arg(buf, a25);
                    break;
                case 26:
                    encode_arg(buf, a26);
                    break;
                case 27:
                    encode_arg(buf, a27);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21,
            typename A22, typename A23, typename A24, typename A25, typename A26, typename A27, typename A28>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21, A22& a22, A23& a23, A24& a24, A25& a25, A26& a26, A27& a27, A28& a28)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 29; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
                case 12:
                    encode_arg(buf, a12);
                    break;
                case 13:
                    encode_arg(buf, a13);
                    break;
                case 14:
                    encode_arg(buf, a14);
                    break;
                case 15:
                    encode_arg(buf, a15);
                    break;
                case 16:
                    encode_arg(buf, a16);
                    break;
                case 17:
                    encode_arg(buf, a17);
                    break;
                case 18:
                    encode_arg(buf, a18);
                    break;
                case 19:
                    encode_arg(buf, a19);
                    break;
                case 20:
                    encode_arg(buf, a20);
                    break;
                case 21:
                    encode_arg(buf, a21);
                    break;
                case 22:
                    encode_arg(buf, a22);
                    break;
                case 23:
                    encode_arg(buf, a23);
                    break;
                case 24:
                    encode_arg(buf, a24);
                    break;
                case 25:
                    encode_arg(buf, a25);
                    break;
                case 26:
                    encode_arg(buf, a26);
                    break;
                case 27:
                    encode_arg(buf, a27);
                    break;
                case 28:
                    encode_arg(buf, a28);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21,
            typename A22, typename A23, typename A24, typename A25, typename A26, typename A27, typename A28,
            typename A29>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21, A22& a22, A23& a23, A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 30; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
                case 12:
                    encode_arg(buf, a12);
                    break;
                case 13:
                    encode_arg(buf, a13);
                    break;
                case 14:
                    encode_arg(buf, a14);
                    break;
                case 15:
                    encode_arg(buf, a15);
                    break;
                case 16:
                    encode_arg(buf, a16);
                    break;
                case 17:
                    encode_arg(buf, a17);
                    break;
                case 18:
                    encode_arg(buf, a18);
                    break;
                case 19:
                    encode_arg(buf, a19);
                    break;
                case 20:
                    encode_arg(buf, a20);
                    break;
                case 21:
                    encode_arg(buf, a21);
                    break;
                case 22:
                    encode_arg(buf, a22);
                    break;
                case 23:
                    encode_arg(buf, a23);
                    break;
                case 24:
                    encode_arg(buf, a24);
                    break;
                case 25:
                    encode_arg(buf, a25);
                    break;
                case 26:
                    encode_arg(buf, a26);
                    break;
                case 27:
                    encode_arg(buf, a27);
                    break;
                case 28:
                    encode_arg(buf, a28);
                    break;
                case 29:
                    encode_arg(buf, a29);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21,
            typename A22, typename A23, typename A24, typename A25, typename A26, typename A27, typename A28,
            typename A29, typename A30>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21, A22& a22, A23& a23, A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
            A30& a30)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 31; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
                case 12:
                    encode_arg(buf, a12);
                    break;
                case 13:
                    encode_arg(buf, a13);
                    break;
                case 14:
                    encode_arg(buf, a14);
                    break;
                case 15:
                    encode_arg(buf, a15);
                    break;
                case 16:
                    encode_arg(buf, a16);
                    break;
                case 17:
                    encode_arg(buf, a17);
                    break;
                case 18:
                    encode_arg(buf, a18);
                    break;
                case 19:
                    encode_arg(buf, a19);
                    break;
                case 20:
                    encode_arg(buf, a20);
                    break;
                case 21:
                    encode_arg(buf, a21);
                    break;
                case 22:
                    encode_arg(buf, a22);
                    break;
                case 23:
                    encode_arg(buf, a23);
                    break;
                case 24:
                    encode_arg(buf, a24);
                    break;
                case 25:
                    encode_arg(buf, a25);
                    break;
                case 26:
                    encode_arg(buf, a26);
                    break;
                case 27:
                    encode_arg(buf, a27);
                    break;
                case 28:
                    encode_arg(buf, a28);
                    break;
                case 29:
                    encode_arg(buf, a29);
                    break;
                case 30:
                    encode_arg(buf, a30);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
    template<typename A0, typename A1, typename A2, typename A3, typename A4, typename A5, typename A6, typename A7,
            typename A8, typename A9, typename A10, typename A11, typename A12, typename A13, typename A14,
            typename A15, typename A16, typename A17, typename A18, typename A19, typename A20, typename A21,
            typename A22, typename A23, typename A24, typename A25, typename A26, typename A27, typename A28,
            typename A29, typename A30, typename A31>
    inline bool encode_arg_with_tag(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4, A5& a5, A6& a6, A7& a7, A8& a8,
            A9& a9, A10& a10, A11& a11, A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17, A18& a18, A19& a19,
            A20& a20, A21& a21, A22& a22, A23& a23, A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
            A30& a30, A31& a31)
    {
        uint32_t cursor = buf.GetWriteIndex();
        BufferHelper::WriteFixUInt32(buf, 1);
        for (uint32_t i = 0; i < 32; i++)
        {
            BufferHelper::WriteVarUInt32(buf, i);
            switch (i)
            {
                case 0:
                    encode_arg(buf, a0);
                    break;
                case 1:
                    encode_arg(buf, a1);
                    break;
                case 2:
                    encode_arg(buf, a2);
                    break;
                case 3:
                    encode_arg(buf, a3);
                    break;
                case 4:
                    encode_arg(buf, a4);
                    break;
                case 5:
                    encode_arg(buf, a5);
                    break;
                case 6:
                    encode_arg(buf, a6);
                    break;
                case 7:
                    encode_arg(buf, a7);
                    break;
                case 8:
                    encode_arg(buf, a8);
                    break;
                case 9:
                    encode_arg(buf, a9);
                    break;
                case 10:
                    encode_arg(buf, a10);
                    break;
                case 11:
                    encode_arg(buf, a11);
                    break;
                case 12:
                    encode_arg(buf, a12);
                    break;
                case 13:
                    encode_arg(buf, a13);
                    break;
                case 14:
                    encode_arg(buf, a14);
                    break;
                case 15:
                    encode_arg(buf, a15);
                    break;
                case 16:
                    encode_arg(buf, a16);
                    break;
                case 17:
                    encode_arg(buf, a17);
                    break;
                case 18:
                    encode_arg(buf, a18);
                    break;
                case 19:
                    encode_arg(buf, a19);
                    break;
                case 20:
                    encode_arg(buf, a20);
                    break;
                case 21:
                    encode_arg(buf, a21);
                    break;
                case 22:
                    encode_arg(buf, a22);
                    break;
                case 23:
                    encode_arg(buf, a23);
                    break;
                case 24:
                    encode_arg(buf, a24);
                    break;
                case 25:
                    encode_arg(buf, a25);
                    break;
                case 26:
                    encode_arg(buf, a26);
                    break;
                case 27:
                    encode_arg(buf, a27);
                    break;
                case 28:
                    encode_arg(buf, a28);
                    break;
                case 29:
                    encode_arg(buf, a29);
                    break;
                case 30:
                    encode_arg(buf, a30);
                    break;
                case 31:
                    encode_arg(buf, a31);
                    break;
            }
            uint32_t end = buf.GetWriteIndex();
            buf.SetWriteIndex(cursor);
            BufferHelper::WriteFixUInt32(buf, end - cursor - 4);
            buf.SetWriteIndex(end);
        }
        return true;
    }
}

#endif /* STRUCT_CODE_TEMPLATES2_HPP_ */
