// Copyright 2018 Ulf Adams
//
// The contents of this file may be used under the terms of the Apache License,
// Version 2.0.
//
//    (See accompanying file LICENSE-Apache or copy at
//     http://www.apache.org/licenses/LICENSE-2.0)
//
// Alternatively, the contents of this file may be used under the terms of
// the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE-Boost or copy at
//     https://www.boost.org/LICENSE_1_0.txt)
//
// Unless required by applicable law or agreed to in writing, this software
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.

// Ryu d2d from https://github.com/ulfjack/ryu/blob/master/ryu/d2s.c, ported to a cuDF transform UDF.
// The tables of ryu_tables.cu are prepended. cuDF renames the identifier before the first parenthesis
// in the source to its own UDF name, so the UDF must come first after the tables and the helpers are
// lambdas inside it. A UDF produces a single INT32 per row, so RYU_OUTPUT selects one part of the
// shortest decimal representation:
//   0: fraction digits 1 to 8
//   1: fraction digits 9 to 16
//   2: (decimal exponent + RYU_EXPONENT_BIAS) << RYU_EXPONENT_SHIFT | leading digit << RYU_LEAD_SHIFT
//      | number of fraction digits, which is at least 1
// Fraction digits are left-aligned, so trailing positions are zero. Zero, infinities and NaN produce
// the parts of 0.0E0, the caller replaces them. The RYU_ tokens are replaced with numbers before
// compilation.
__device__ inline void ryu_udf(int* out, double value)
{
  unsigned long long bits = __double_as_longlong(value);
  unsigned long long ieee_mantissa = bits & ((1ull << 52) - 1);
  unsigned int ieee_exponent = (unsigned int) ((bits >> 52) & 0x7ffu);
  if (ieee_exponent == 0x7ffu || (ieee_exponent == 0 && ieee_mantissa == 0)) {
    *out = RYU_OUTPUT == 2 ? (RYU_EXPONENT_BIAS << RYU_EXPONENT_SHIFT) | 1 : 0;
    return;
  }
  auto pow5bits = [](int e) -> int { return (int) ((((unsigned int) e) * 1217359u) >> 19) + 1; };
  auto log10_pow2 = [](int e) -> unsigned int { return (((unsigned int) e) * 78913u) >> 18; };
  auto log10_pow5 = [](int e) -> unsigned int { return (((unsigned int) e) * 732923u) >> 20; };
  auto pow5_factor = [](unsigned long long v) -> unsigned int {
    unsigned int count = 0;
    for (;;) {
      v *= 14757395258967641293ull;
      if (v > 3689348814741910323ull) { break; }
      ++count;
    }
    return count;
  };
  auto mul_shift = [](unsigned long long m, unsigned long long const* mul, int j) -> unsigned long long {
    unsigned long long high1 = __umul64hi(m, mul[1]);
    unsigned long long low1 = m * mul[1];
    unsigned long long high0 = __umul64hi(m, mul[0]);
    unsigned long long sum = high0 + low1;
    if (sum < high0) { ++high1; }
    int dist = j - 64;
    return (high1 << (64 - dist)) | (sum >> dist);
  };

  int e2;
  unsigned long long m2;
  if (ieee_exponent == 0) {
    e2 = 1 - 1023 - 52 - 2;
    m2 = ieee_mantissa;
  }
  else {
    e2 = (int) ieee_exponent - 1023 - 52 - 2;
    m2 = (1ull << 52) | ieee_mantissa;
  }
  bool accept_bounds = (m2 & 1) == 0;
  unsigned long long mv = 4 * m2;
  unsigned int mm_shift = ieee_mantissa != 0 || ieee_exponent <= 1;

  unsigned long long vr, vp, vm;
  int e10;
  bool vm_trailing_zeros = false;
  bool vr_trailing_zeros = false;
  if (e2 >= 0) {
    unsigned int q = log10_pow2(e2) - (e2 > 3);
    e10 = (int) q;
    int k = 125 + pow5bits((int) q) - 1;
    int i = -e2 + (int) q + k;
    vr = mul_shift(4 * m2, RYU_POW5_INV_SPLIT[q], i);
    vp = mul_shift(4 * m2 + 2, RYU_POW5_INV_SPLIT[q], i);
    vm = mul_shift(4 * m2 - 1 - mm_shift, RYU_POW5_INV_SPLIT[q], i);
    if (q <= 21) {
      if (mv % 5 == 0) {
        vr_trailing_zeros = pow5_factor(mv) >= q;
      }
      else if (accept_bounds) {
        vm_trailing_zeros = pow5_factor(mv - 1 - mm_shift) >= q;
      }
      else {
        vp -= pow5_factor(mv + 2) >= q;
      }
    }
  }
  else {
    unsigned int q = log10_pow5(-e2) - (-e2 > 1);
    e10 = (int) q + e2;
    int i = -e2 - (int) q;
    int k = pow5bits(i) - 125;
    int j = (int) q - k;
    vr = mul_shift(4 * m2, RYU_POW5_SPLIT[i], j);
    vp = mul_shift(4 * m2 + 2, RYU_POW5_SPLIT[i], j);
    vm = mul_shift(4 * m2 - 1 - mm_shift, RYU_POW5_SPLIT[i], j);
    if (q <= 1) {
      vr_trailing_zeros = true;
      if (accept_bounds) {
        vm_trailing_zeros = mm_shift == 1;
      }
      else {
        --vp;
      }
    }
    else if (q < 63) {
      vr_trailing_zeros = (mv & ((1ull << q) - 1)) == 0;
    }
  }

  int removed = 0;
  unsigned int last_removed_digit = 0;
  unsigned long long output;
  if (vm_trailing_zeros || vr_trailing_zeros) {
    for (;;) {
      if (vp / 10 <= vm / 10) { break; }
      vm_trailing_zeros &= vm % 10 == 0;
      vr_trailing_zeros &= last_removed_digit == 0;
      last_removed_digit = (unsigned int) (vr % 10);
      vr /= 10;
      vp /= 10;
      vm /= 10;
      ++removed;
    }
    if (vm_trailing_zeros) {
      for (;;) {
        if (vm % 10 != 0) { break; }
        vr_trailing_zeros &= last_removed_digit == 0;
        last_removed_digit = (unsigned int) (vr % 10);
        vr /= 10;
        vp /= 10;
        vm /= 10;
        ++removed;
      }
    }
    if (vr_trailing_zeros && last_removed_digit == 5 && vr % 2 == 0) {
      last_removed_digit = 4;
    }
    output = vr + ((vr == vm && (!accept_bounds || !vm_trailing_zeros)) || last_removed_digit >= 5);
  }
  else {
    bool round_up = false;
    if (vp / 100 > vm / 100) {
      round_up = vr % 100 >= 50;
      vr /= 100;
      vp /= 100;
      vm /= 100;
      removed += 2;
    }
    for (;;) {
      if (vp / 10 <= vm / 10) { break; }
      round_up = vr % 10 >= 5;
      vr /= 10;
      vp /= 10;
      vm /= 10;
      ++removed;
    }
    output = vr + (vr == vm || round_up);
  }
  int exponent = e10 + removed;

  // Java prints digits other than the shortest for many powers of two, where the interval rounding to
  // the value is narrower below it than above, and for the smallest subnormals, where the shortest
  // form may have a single digit. Their digits come from a table of Java's results, prepended to this
  // source: entries 0 to RYU_SMALL_SUBNORMALS - 1 are the subnormals with these bits, the next ones the
  // subnormal and then normal powers of two above them. A zero entry, used where Java's digits do not
  // fit in the output, keeps the shortest digits.
  unsigned long long magnitude = bits & ~(1ull << 63);
  int index = -1;
  if (magnitude < RYU_SMALL_SUBNORMALS) {
    index = (int) magnitude;
  }
  else if (ieee_exponent == 0 ? (ieee_mantissa & (ieee_mantissa - 1)) == 0 : ieee_mantissa == 0) {
    index = RYU_SMALL_SUBNORMALS + (ieee_exponent == 0 ? 63 - __clzll(ieee_mantissa) : (int) ieee_exponent + 51) - RYU_SMALL_SUBNORMAL_POWERS;
  }
  if (index >= 0 && RYU_SPECIAL_DIGITS[index] != 0) {
    output = RYU_SPECIAL_DIGITS[index];
    exponent = RYU_SPECIAL_EXPONENTS[index];
  }

  while (output >= 10 && output % 10 == 0) {
    output /= 10;
    ++exponent;
  }
  int length = 1;
  unsigned long long scale = 1;
  while (output / scale >= 10) {
    scale *= 10;
    ++length;
  }
  unsigned long long fraction = output - (output / scale) * scale;
  for (int digit = length - 1; digit < 16; digit++) {
    fraction *= 10;
  }
  int result;
  switch (RYU_OUTPUT) {
    case 0: result = (int) (fraction / 100000000ull); break;
    case 1: result = (int) (fraction % 100000000ull); break;
    default:
      result = ((exponent + length - 1 + RYU_EXPONENT_BIAS) << RYU_EXPONENT_SHIFT)
          | ((int) (output / scale) << RYU_LEAD_SHIFT)
          | (length > 1 ? length - 1 : 1);
      break;
  }
  *out = result;
}
