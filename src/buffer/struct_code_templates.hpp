#ifndef STRUCT_CODE_TEMPLATES_HPP_
#define STRUCT_CODE_TEMPLATES_HPP_

namespace ardb
{
	template<typename A0, typename A1>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1)
	{
		return decode_arg(buf, a0) && decode_arg(buf, a1);
	}
	template<typename A0, typename A1, typename A2>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2)
	{
		return decode_arg(buf, a0, a1) && decode_arg(buf, a2);
	}
	template<typename A0, typename A1, typename A2, typename A3>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3)
	{
		return decode_arg(buf, a0, a1, a2) && decode_arg(buf, a3);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4)
	{
		return decode_arg(buf, a0, a1, a2, a3) && decode_arg(buf, a4);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4) && decode_arg(buf, a5);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5) && decode_arg(buf, a6);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6)
		        && decode_arg(buf, a7);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7)
		        && decode_arg(buf, a8);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8)
		        && decode_arg(buf, a9);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9)
		        && decode_arg(buf, a10);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)
		        && decode_arg(buf, a11);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11)
		        && decode_arg(buf, a12);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12) && decode_arg(buf, a13);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13, typename A14>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13) && decode_arg(buf, a14);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14) && decode_arg(buf, a15);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15) && decode_arg(buf, a16);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16) && decode_arg(buf, a17);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17, typename A18>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17) && decode_arg(buf, a18);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18) && decode_arg(buf, a19);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19) && decode_arg(buf, a20);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20)
		        && decode_arg(buf, a21);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21, typename A22>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21)
		        && decode_arg(buf, a22);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22)
		        && decode_arg(buf, a23);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23)
		        && decode_arg(buf, a24);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24)
		        && decode_arg(buf, a25);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25, typename A26>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25) && decode_arg(buf, a26);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26) && decode_arg(buf, a27);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27) && decode_arg(buf, a28);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28) && decode_arg(buf, a29);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29, typename A30>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29) && decode_arg(buf, a30);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30) && decode_arg(buf, a31);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31) && decode_arg(buf, a32);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32) && decode_arg(buf, a33);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33, typename A34>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33)
		        && decode_arg(buf, a34);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34)
		        && decode_arg(buf, a35);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35)
		        && decode_arg(buf, a36);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36)
		        && decode_arg(buf, a37);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37, typename A38>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37)
		        && decode_arg(buf, a38);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38) && decode_arg(buf, a39);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39) && decode_arg(buf, a40);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40) && decode_arg(buf, a41);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41, typename A42>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41) && decode_arg(buf, a42);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42) && decode_arg(buf, a43);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43) && decode_arg(buf, a44);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44) && decode_arg(buf, a45);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45, typename A46>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45) && decode_arg(buf, a46);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46)
		        && decode_arg(buf, a47);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47)
		        && decode_arg(buf, a48);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48)
		        && decode_arg(buf, a49);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49, typename A50>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49)
		        && decode_arg(buf, a50);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50)
		        && decode_arg(buf, a51);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51) && decode_arg(buf, a52);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52) && decode_arg(buf, a53);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53, typename A54>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52, a53) && decode_arg(buf, a54);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52, a53, a54) && decode_arg(buf, a55);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52, a53, a54, a55) && decode_arg(buf, a56);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52, a53, a54, a55, a56) && decode_arg(buf, a57);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57, typename A58>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52, a53, a54, a55, a56, a57) && decode_arg(buf, a58);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57,
	        typename A58, typename A59>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58, A59& a59)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52, a53, a54, a55, a56, a57, a58) && decode_arg(buf, a59);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57,
	        typename A58, typename A59, typename A60>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58, A59& a59,
	        A60& a60)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52, a53, a54, a55, a56, a57, a58, a59)
		        && decode_arg(buf, a60);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57,
	        typename A58, typename A59, typename A60, typename A61>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58, A59& a59,
	        A60& a60, A61& a61)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52, a53, a54, a55, a56, a57, a58, a59, a60)
		        && decode_arg(buf, a61);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57,
	        typename A58, typename A59, typename A60, typename A61, typename A62>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58, A59& a59,
	        A60& a60, A61& a61, A62& a62)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52, a53, a54, a55, a56, a57, a58, a59, a60, a61)
		        && decode_arg(buf, a62);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57,
	        typename A58, typename A59, typename A60, typename A61,
	        typename A62, typename A63>
	inline bool decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58, A59& a59,
	        A60& a60, A61& a61, A62& a62, A63& a63)
	{
		return decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52, a53, a54, a55, a56, a57, a58, a59, a60, a61, a62)
		        && decode_arg(buf, a63);
	}

	template<typename A0, typename A1>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1)
	{
		return encode_arg(buf, a0) && encode_arg(buf, a1);
	}
	template<typename A0, typename A1, typename A2>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2)
	{
		return encode_arg(buf, a0, a1) && encode_arg(buf, a2);
	}
	template<typename A0, typename A1, typename A2, typename A3>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3)
	{
		return encode_arg(buf, a0, a1, a2) && encode_arg(buf, a3);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4)
	{
		return encode_arg(buf, a0, a1, a2, a3) && encode_arg(buf, a4);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4) && encode_arg(buf, a5);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5) && encode_arg(buf, a6);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6)
		        && encode_arg(buf, a7);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7)
		        && encode_arg(buf, a8);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8)
		        && encode_arg(buf, a9);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9)
		        && encode_arg(buf, a10);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)
		        && encode_arg(buf, a11);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11)
		        && encode_arg(buf, a12);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12) && encode_arg(buf, a13);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13, typename A14>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13) && encode_arg(buf, a14);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14) && encode_arg(buf, a15);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15) && encode_arg(buf, a16);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16) && encode_arg(buf, a17);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17, typename A18>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17) && encode_arg(buf, a18);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18) && encode_arg(buf, a19);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19) && encode_arg(buf, a20);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20)
		        && encode_arg(buf, a21);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21, typename A22>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21)
		        && encode_arg(buf, a22);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22)
		        && encode_arg(buf, a23);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23)
		        && encode_arg(buf, a24);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24)
		        && encode_arg(buf, a25);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25, typename A26>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25) && encode_arg(buf, a26);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26) && encode_arg(buf, a27);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27) && encode_arg(buf, a28);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28) && encode_arg(buf, a29);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29, typename A30>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29) && encode_arg(buf, a30);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30) && encode_arg(buf, a31);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31) && encode_arg(buf, a32);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32) && encode_arg(buf, a33);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33, typename A34>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33)
		        && encode_arg(buf, a34);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34)
		        && encode_arg(buf, a35);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35)
		        && encode_arg(buf, a36);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36)
		        && encode_arg(buf, a37);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37, typename A38>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37)
		        && encode_arg(buf, a38);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38) && encode_arg(buf, a39);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39) && encode_arg(buf, a40);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40) && encode_arg(buf, a41);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41, typename A42>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41) && encode_arg(buf, a42);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42) && encode_arg(buf, a43);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43) && encode_arg(buf, a44);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44) && encode_arg(buf, a45);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45, typename A46>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45) && encode_arg(buf, a46);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46)
		        && encode_arg(buf, a47);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47)
		        && encode_arg(buf, a48);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48)
		        && encode_arg(buf, a49);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49, typename A50>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49)
		        && encode_arg(buf, a50);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50)
		        && encode_arg(buf, a51);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51) && encode_arg(buf, a52);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52) && encode_arg(buf, a53);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53, typename A54>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52, a53) && encode_arg(buf, a54);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52, a53, a54) && encode_arg(buf, a55);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52, a53, a54, a55) && encode_arg(buf, a56);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52, a53, a54, a55, a56) && encode_arg(buf, a57);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57, typename A58>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52, a53, a54, a55, a56, a57) && encode_arg(buf, a58);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57,
	        typename A58, typename A59>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58, A59& a59)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52, a53, a54, a55, a56, a57, a58) && encode_arg(buf, a59);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57,
	        typename A58, typename A59, typename A60>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58, A59& a59,
	        A60& a60)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52, a53, a54, a55, a56, a57, a58, a59)
		        && encode_arg(buf, a60);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57,
	        typename A58, typename A59, typename A60, typename A61>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58, A59& a59,
	        A60& a60, A61& a61)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52, a53, a54, a55, a56, a57, a58, a59, a60)
		        && encode_arg(buf, a61);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57,
	        typename A58, typename A59, typename A60, typename A61, typename A62>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58, A59& a59,
	        A60& a60, A61& a61, A62& a62)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52, a53, a54, a55, a56, a57, a58, a59, a60, a61)
		        && encode_arg(buf, a62);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57,
	        typename A58, typename A59, typename A60, typename A61,
	        typename A62, typename A63>
	inline bool encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3, A4& a4,
	        A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58, A59& a59,
	        A60& a60, A61& a61, A62& a62, A63& a63)
	{
		return encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11,
		        a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24,
		        a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36, a37,
		        a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49, a50,
		        a51, a52, a53, a54, a55, a56, a57, a58, a59, a60, a61, a62)
		        && encode_arg(buf, a63);
	}

	template<typename A0, typename A1>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1)
	{
		return fix_decode_arg(buf, a0) && fix_decode_arg(buf, a1);
	}
	template<typename A0, typename A1, typename A2>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2)
	{
		return fix_decode_arg(buf, a0, a1) && fix_decode_arg(buf, a2);
	}
	template<typename A0, typename A1, typename A2, typename A3>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3)
	{
		return fix_decode_arg(buf, a0, a1, a2) && fix_decode_arg(buf, a3);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3) && fix_decode_arg(buf, a4);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4)
		        && fix_decode_arg(buf, a5);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5)
		        && fix_decode_arg(buf, a6);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6)
		        && fix_decode_arg(buf, a7);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7)
		        && fix_decode_arg(buf, a8);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8)
		        && fix_decode_arg(buf, a9);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9)
		        && fix_decode_arg(buf, a10);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)
		        && fix_decode_arg(buf, a11);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11) && fix_decode_arg(buf, a12);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12) && fix_decode_arg(buf, a13);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13, typename A14>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13) && fix_decode_arg(buf, a14);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14) && fix_decode_arg(buf, a15);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15) && fix_decode_arg(buf, a16);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16) && fix_decode_arg(buf, a17);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17, typename A18>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17) && fix_decode_arg(buf, a18);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18)
		        && fix_decode_arg(buf, a19);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19)
		        && fix_decode_arg(buf, a20);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20)
		        && fix_decode_arg(buf, a21);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21, typename A22>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21)
		        && fix_decode_arg(buf, a22);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22)
		        && fix_decode_arg(buf, a23);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23)
		        && fix_decode_arg(buf, a24);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24) && fix_decode_arg(buf, a25);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25, typename A26>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25) && fix_decode_arg(buf, a26);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26) && fix_decode_arg(buf, a27);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27) && fix_decode_arg(buf, a28);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28) && fix_decode_arg(buf, a29);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29, typename A30>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29) && fix_decode_arg(buf, a30);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30) && fix_decode_arg(buf, a31);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31)
		        && fix_decode_arg(buf, a32);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32)
		        && fix_decode_arg(buf, a33);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33, typename A34>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33)
		        && fix_decode_arg(buf, a34);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34)
		        && fix_decode_arg(buf, a35);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35)
		        && fix_decode_arg(buf, a36);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36)
		        && fix_decode_arg(buf, a37);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37, typename A38>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37) && fix_decode_arg(buf, a38);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38) && fix_decode_arg(buf, a39);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39) && fix_decode_arg(buf, a40);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40) && fix_decode_arg(buf, a41);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41, typename A42>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41) && fix_decode_arg(buf, a42);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42) && fix_decode_arg(buf, a43);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43) && fix_decode_arg(buf, a44);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44)
		        && fix_decode_arg(buf, a45);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45, typename A46>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45)
		        && fix_decode_arg(buf, a46);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46)
		        && fix_decode_arg(buf, a47);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47)
		        && fix_decode_arg(buf, a48);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48)
		        && fix_decode_arg(buf, a49);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49, typename A50>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49)
		        && fix_decode_arg(buf, a50);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50) && fix_decode_arg(buf, a51);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51) && fix_decode_arg(buf, a52);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52) && fix_decode_arg(buf, a53);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53, typename A54>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52, a53) && fix_decode_arg(buf, a54);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52, a53, a54) && fix_decode_arg(buf, a55);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52, a53, a54, a55) && fix_decode_arg(buf, a56);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52, a53, a54, a55, a56) && fix_decode_arg(buf, a57);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57, typename A58>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52, a53, a54, a55, a56, a57)
		        && fix_decode_arg(buf, a58);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57,
	        typename A58, typename A59>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58, A59& a59)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52, a53, a54, a55, a56, a57, a58)
		        && fix_decode_arg(buf, a59);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57,
	        typename A58, typename A59, typename A60>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58, A59& a59,
	        A60& a60)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52, a53, a54, a55, a56, a57, a58, a59)
		        && fix_decode_arg(buf, a60);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57,
	        typename A58, typename A59, typename A60, typename A61>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58, A59& a59,
	        A60& a60, A61& a61)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52, a53, a54, a55, a56, a57, a58, a59, a60)
		        && fix_decode_arg(buf, a61);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57,
	        typename A58, typename A59, typename A60, typename A61, typename A62>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58, A59& a59,
	        A60& a60, A61& a61, A62& a62)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52, a53, a54, a55, a56, a57, a58, a59, a60, a61)
		        && fix_decode_arg(buf, a62);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57,
	        typename A58, typename A59, typename A60, typename A61,
	        typename A62, typename A63>
	inline bool fix_decode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58, A59& a59,
	        A60& a60, A61& a61, A62& a62, A63& a63)
	{
		return fix_decode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52, a53, a54, a55, a56, a57, a58, a59, a60, a61, a62)
		        && fix_decode_arg(buf, a63);
	}

	template<typename A0, typename A1>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1)
	{
		return fix_encode_arg(buf, a0) && fix_encode_arg(buf, a1);
	}
	template<typename A0, typename A1, typename A2>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2)
	{
		return fix_encode_arg(buf, a0, a1) && fix_encode_arg(buf, a2);
	}
	template<typename A0, typename A1, typename A2, typename A3>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3)
	{
		return fix_encode_arg(buf, a0, a1, a2) && fix_encode_arg(buf, a3);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3) && fix_encode_arg(buf, a4);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4)
		        && fix_encode_arg(buf, a5);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5)
		        && fix_encode_arg(buf, a6);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6)
		        && fix_encode_arg(buf, a7);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7)
		        && fix_encode_arg(buf, a8);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8)
		        && fix_encode_arg(buf, a9);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9)
		        && fix_encode_arg(buf, a10);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)
		        && fix_encode_arg(buf, a11);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11) && fix_encode_arg(buf, a12);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12) && fix_encode_arg(buf, a13);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13, typename A14>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13) && fix_encode_arg(buf, a14);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14) && fix_encode_arg(buf, a15);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15) && fix_encode_arg(buf, a16);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16) && fix_encode_arg(buf, a17);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17, typename A18>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17) && fix_encode_arg(buf, a18);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18)
		        && fix_encode_arg(buf, a19);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19)
		        && fix_encode_arg(buf, a20);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20)
		        && fix_encode_arg(buf, a21);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21, typename A22>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21)
		        && fix_encode_arg(buf, a22);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22)
		        && fix_encode_arg(buf, a23);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23)
		        && fix_encode_arg(buf, a24);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24) && fix_encode_arg(buf, a25);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25, typename A26>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25) && fix_encode_arg(buf, a26);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26) && fix_encode_arg(buf, a27);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27) && fix_encode_arg(buf, a28);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28) && fix_encode_arg(buf, a29);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29, typename A30>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29) && fix_encode_arg(buf, a30);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30) && fix_encode_arg(buf, a31);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31)
		        && fix_encode_arg(buf, a32);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32)
		        && fix_encode_arg(buf, a33);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33, typename A34>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33)
		        && fix_encode_arg(buf, a34);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34)
		        && fix_encode_arg(buf, a35);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35)
		        && fix_encode_arg(buf, a36);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36)
		        && fix_encode_arg(buf, a37);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37, typename A38>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37) && fix_encode_arg(buf, a38);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38) && fix_encode_arg(buf, a39);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39) && fix_encode_arg(buf, a40);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40) && fix_encode_arg(buf, a41);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41, typename A42>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41) && fix_encode_arg(buf, a42);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42) && fix_encode_arg(buf, a43);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43) && fix_encode_arg(buf, a44);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44)
		        && fix_encode_arg(buf, a45);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45, typename A46>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45)
		        && fix_encode_arg(buf, a46);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46)
		        && fix_encode_arg(buf, a47);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47)
		        && fix_encode_arg(buf, a48);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48)
		        && fix_encode_arg(buf, a49);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49, typename A50>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49)
		        && fix_encode_arg(buf, a50);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50) && fix_encode_arg(buf, a51);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51) && fix_encode_arg(buf, a52);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52) && fix_encode_arg(buf, a53);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53, typename A54>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52, a53) && fix_encode_arg(buf, a54);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52, a53, a54) && fix_encode_arg(buf, a55);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52, a53, a54, a55) && fix_encode_arg(buf, a56);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52, a53, a54, a55, a56) && fix_encode_arg(buf, a57);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57, typename A58>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52, a53, a54, a55, a56, a57)
		        && fix_encode_arg(buf, a58);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57,
	        typename A58, typename A59>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58, A59& a59)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52, a53, a54, a55, a56, a57, a58)
		        && fix_encode_arg(buf, a59);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57,
	        typename A58, typename A59, typename A60>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58, A59& a59,
	        A60& a60)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52, a53, a54, a55, a56, a57, a58, a59)
		        && fix_encode_arg(buf, a60);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57,
	        typename A58, typename A59, typename A60, typename A61>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58, A59& a59,
	        A60& a60, A61& a61)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52, a53, a54, a55, a56, a57, a58, a59, a60)
		        && fix_encode_arg(buf, a61);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57,
	        typename A58, typename A59, typename A60, typename A61, typename A62>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58, A59& a59,
	        A60& a60, A61& a61, A62& a62)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52, a53, a54, a55, a56, a57, a58, a59, a60, a61)
		        && fix_encode_arg(buf, a62);
	}
	template<typename A0, typename A1, typename A2, typename A3, typename A4,
	        typename A5, typename A6, typename A7, typename A8, typename A9,
	        typename A10, typename A11, typename A12, typename A13,
	        typename A14, typename A15, typename A16, typename A17,
	        typename A18, typename A19, typename A20, typename A21,
	        typename A22, typename A23, typename A24, typename A25,
	        typename A26, typename A27, typename A28, typename A29,
	        typename A30, typename A31, typename A32, typename A33,
	        typename A34, typename A35, typename A36, typename A37,
	        typename A38, typename A39, typename A40, typename A41,
	        typename A42, typename A43, typename A44, typename A45,
	        typename A46, typename A47, typename A48, typename A49,
	        typename A50, typename A51, typename A52, typename A53,
	        typename A54, typename A55, typename A56, typename A57,
	        typename A58, typename A59, typename A60, typename A61,
	        typename A62, typename A63>
	inline bool fix_encode_arg(Buffer& buf, A0& a0, A1& a1, A2& a2, A3& a3,
	        A4& a4, A5& a5, A6& a6, A7& a7, A8& a8, A9& a9, A10& a10, A11& a11,
	        A12& a12, A13& a13, A14& a14, A15& a15, A16& a16, A17& a17,
	        A18& a18, A19& a19, A20& a20, A21& a21, A22& a22, A23& a23,
	        A24& a24, A25& a25, A26& a26, A27& a27, A28& a28, A29& a29,
	        A30& a30, A31& a31, A32& a32, A33& a33, A34& a34, A35& a35,
	        A36& a36, A37& a37, A38& a38, A39& a39, A40& a40, A41& a41,
	        A42& a42, A43& a43, A44& a44, A45& a45, A46& a46, A47& a47,
	        A48& a48, A49& a49, A50& a50, A51& a51, A52& a52, A53& a53,
	        A54& a54, A55& a55, A56& a56, A57& a57, A58& a58, A59& a59,
	        A60& a60, A61& a61, A62& a62, A63& a63)
	{
		return fix_encode_arg(buf, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
		        a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23,
		        a24, a25, a26, a27, a28, a29, a30, a31, a32, a33, a34, a35, a36,
		        a37, a38, a39, a40, a41, a42, a43, a44, a45, a46, a47, a48, a49,
		        a50, a51, a52, a53, a54, a55, a56, a57, a58, a59, a60, a61, a62)
		        && fix_encode_arg(buf, a63);
	}
}

#endif /* STRUCT_CODE_TEMPLATES_HPP_ */
