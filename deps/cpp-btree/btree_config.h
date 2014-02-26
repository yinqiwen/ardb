// Copyright (c) 2013, Tencent Inc.
// All rights reserved.
//
// Author: CHEN Feng <chen3feng@gmail.com>
// Created: 2013-04-08

#ifndef CPP_BTREE_CONFIG_H
#define CPP_BTREE_CONFIG_H
#pragma once

#if defined(__GXX_EXPERIMENTAL_CXX0X__) || (__cplusplus >= 201103L) || \
    (_MSC_VER >= 1600)
#define CPP_BTREE_CXX11 1
#endif

#if __GNUC__
#define GNUC_VERSION (__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__)
#if GNUC_VERSION >= 40100
#define HAVE_STD_TR1
#endif
#endif

// Config the type_traits
#ifdef CPP_BTREE_CXX11
# define CPP_BTREE_TYPE_TRAITS_HEADER <type_traits>
# define CPP_BTREE_TYPE_TRAITS_NS std
#else  // CPP_BTREE_CXX11
// For pre-c++11 compilers, tr1::type_traits is usually available.
# ifdef _MSC_VER
  #define CPP_BTREE_TYPE_TRAITS_HEADER <type_traits>
# elif defined(HAVE_STD_TR1) && !defined(__APPLE__)
  #define CPP_BTREE_TYPE_TRAITS_HEADER <tr1/type_traits>
#  else
#  define CPP_BTREE_TYPE_TRAITS_HEADER <boost/tr1/type_traits.hpp>
# endif
#define CPP_BTREE_TYPE_TRAITS_NS std::tr1
#endif //  CPP_BTREE_CXX11

// Allow user override the size_type of btree containers.
// Define CPP_BTREE_SIZE_TYPE to be size_t make it total compatible with STL.
#ifndef CPP_BTREE_SIZE_TYPE
#define CPP_BTREE_SIZE_TYPE ssize_t
#endif

#endif  // CPP_BTREE_CONFIG_H
