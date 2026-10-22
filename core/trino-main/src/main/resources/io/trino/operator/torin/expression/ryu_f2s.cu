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

// Ryu f2d from https://github.com/ulfjack/ryu/blob/master/ryu/f2s.c, and the exactness check of s2d
// from ryu/s2d.c, ported to a cuDF transform UDF. The tables RYU_POW5_INV_SPLIT and RYU_POW5_SPLIT of
// ryu_tables.cu are prepended. cuDF renames the identifier before the first parenthesis in the source
// to its own UDF name, so the UDF must come first after the tables and the helpers are lambdas inside it.
//
// Matches io.trino.type.RealOperators#castToVarchar, which formats the double parsed from
// Float.toString with at most 7 significant digits: Float.toString's digits are the shortest ones,
// except for a few subnormals where Java needs at least 2 digits, and they are rounded to 7 digits
// by the exact value of the double they parse to, ties rounding half up for magnitudes in
// [2^23, 2^63) and half even elsewhere.
//
// A UDF produces a single INT32 per row, so RYU_OUTPUT selects one part of the result:
//   0: fraction digits 1 to 8, of which at most 6 are used
//   2: (decimal exponent + RYU_EXPONENT_BIAS) << RYU_EXPONENT_SHIFT | leading digit << RYU_LEAD_SHIFT
//      | number of fraction digits, which is at least 1
// Fraction digits are left-aligned, so trailing positions are zero. Zero, infinities and NaN produce
// the parts of 0.0E0, the caller replaces them. The RYU_ tokens are replaced with numbers before
// compilation.
__device__ inline void ryu_udf(int* out, float value)
{
  unsigned int bits = __float_as_uint(value);
  unsigned int ieee_mantissa = bits & ((1u << 23) - 1);
  unsigned int ieee_exponent = (bits >> 23) & 0xffu;
  if (ieee_exponent == 0xffu || (ieee_exponent == 0 && ieee_mantissa == 0)) {
    *out = RYU_OUTPUT == 2 ? (RYU_EXPONENT_BIAS << RYU_EXPONENT_SHIFT) | 1 : 0;
    return;
  }
  auto pow5bits = [](int e) -> int { return (int) ((((unsigned int) e) * 1217359u) >> 19) + 1; };
  auto log2_pow5 = [](int e) -> int { return (int) ((((unsigned int) e) * 1217359u) >> 19); };
  auto log10_pow2 = [](int e) -> unsigned int { return (((unsigned int) e) * 78913u) >> 18; };
  auto log10_pow5 = [](int e) -> unsigned int { return (((unsigned int) e) * 732923u) >> 20; };
  auto pow5_factor = [](unsigned long long v) -> unsigned int {
    unsigned int count = 0;
    while (v % 5 == 0) {
      v /= 5;
      ++count;
    }
    return count;
  };
  auto mul_shift_32 = [](unsigned int m, unsigned long long factor, int shift) -> unsigned int {
    unsigned long long bits0 = (unsigned long long) m * (factor & 0xffffffffull);
    unsigned long long bits1 = (unsigned long long) m * (factor >> 32);
    return (unsigned int) (((bits0 >> 32) + bits1) >> (shift - 32));
  };
  auto mul_shift_64 = [](unsigned long long m, unsigned long long const* mul, int j) -> unsigned long long {
    unsigned long long high1 = __umul64hi(m, mul[1]);
    unsigned long long low1 = m * mul[1];
    unsigned long long high0 = __umul64hi(m, mul[0]);
    unsigned long long sum = high0 + low1;
    if (sum < high0) { ++high1; }
    int dist = j - 64;
    if (dist >= 64) { return high1 >> (dist - 64); }
    if (dist == 0) { return sum; }
    return (high1 << (64 - dist)) | (sum >> dist);
  };

  int e2;
  unsigned int m2;
  if (ieee_exponent == 0) {
    e2 = 1 - 127 - 23 - 2;
    m2 = ieee_mantissa;
  }
  else {
    e2 = (int) ieee_exponent - 127 - 23 - 2;
    m2 = (1u << 23) | ieee_mantissa;
  }
  bool accept_bounds = (m2 & 1) == 0;
  unsigned int mv = 4 * m2;
  unsigned int mp = 4 * m2 + 2;
  unsigned int mm_shift = ieee_mantissa != 0 || ieee_exponent <= 1;
  unsigned int mm = 4 * m2 - 1 - mm_shift;

  unsigned int vr, vp, vm;
  int e10;
  bool vm_trailing_zeros = false;
  bool vr_trailing_zeros = false;
  unsigned int last_removed_digit = 0;
  if (e2 >= 0) {
    unsigned int q = log10_pow2(e2);
    e10 = (int) q;
    int k = 61 + pow5bits((int) q) - 1;
    int i = -e2 + (int) q + k;
    unsigned long long factor = RYU_POW5_INV_SPLIT[q][1] + 1;
    vr = mul_shift_32(mv, factor, i);
    vp = mul_shift_32(mp, factor, i);
    vm = mul_shift_32(mm, factor, i);
    if (q != 0 && (vp - 1) / 10 <= vm / 10) {
      int l = 61 + pow5bits((int) (q - 1)) - 1;
      last_removed_digit = mul_shift_32(mv, RYU_POW5_INV_SPLIT[q - 1][1] + 1, -e2 + (int) q - 1 + l) % 10;
    }
    if (q <= 9) {
      if (mv % 5 == 0) {
        vr_trailing_zeros = pow5_factor(mv) >= q;
      }
      else if (accept_bounds) {
        vm_trailing_zeros = pow5_factor(mm) >= q;
      }
      else {
        vp -= pow5_factor(mp) >= q;
      }
    }
  }
  else {
    unsigned int q = log10_pow5(-e2);
    e10 = (int) q + e2;
    int i = -e2 - (int) q;
    int k = pow5bits(i) - 61;
    int j = (int) q - k;
    vr = mul_shift_32(mv, RYU_POW5_SPLIT[i][1], j);
    vp = mul_shift_32(mp, RYU_POW5_SPLIT[i][1], j);
    vm = mul_shift_32(mm, RYU_POW5_SPLIT[i][1], j);
    if (q != 0 && (vp - 1) / 10 <= vm / 10) {
      j = (int) q - 1 - (pow5bits(i + 1) - 61);
      last_removed_digit = mul_shift_32(mv, RYU_POW5_SPLIT[i + 1][1], j) % 10;
    }
    if (q <= 1) {
      vr_trailing_zeros = true;
      if (accept_bounds) {
        vm_trailing_zeros = mm_shift == 1;
      }
      else {
        --vp;
      }
    }
    else if (q < 31) {
      vr_trailing_zeros = (mv & ((1u << (q - 1)) - 1)) == 0;
    }
  }

  int removed = 0;
  unsigned long long output;
  if (vm_trailing_zeros || vr_trailing_zeros) {
    while (vp / 10 > vm / 10) {
      vm_trailing_zeros &= vm % 10 == 0;
      vr_trailing_zeros &= last_removed_digit == 0;
      last_removed_digit = vr % 10;
      vr /= 10;
      vp /= 10;
      vm /= 10;
      ++removed;
    }
    if (vm_trailing_zeros) {
      while (vm % 10 == 0) {
        vr_trailing_zeros &= last_removed_digit == 0;
        last_removed_digit = vr % 10;
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
    while (vp / 10 > vm / 10) {
      last_removed_digit = vr % 10;
      vr /= 10;
      vp /= 10;
      vm /= 10;
      ++removed;
    }
    output = vr + (vr == vm || last_removed_digit >= 5);
  }
  int exponent = e10 + removed;

  // Java prints at least two significant digits, so for these subnormals it prints the closest
  // 2-digit decimal instead of the 1-digit shortest one
  switch (bits & 0x7fffffffu) {
    case 1: output = 14; exponent = -46; break;
    case 2: output = 28; exponent = -46; break;
    case 3: output = 42; exponent = -46; break;
    case 4: output = 56; exponent = -46; break;
    case 6: output = 84; exponent = -46; break;
    case 7: output = 98; exponent = -46; break;
    case 21: output = 29; exponent = -45; break;
    case 29: output = 41; exponent = -45; break;
    case 71: output = 99; exponent = -45; break;
    default: break;
  }

  while (output % 10 == 0) {
    output /= 10;
    ++exponent;
  }
  int length = 1;
  unsigned long long scale = 1;
  while (output / scale >= 10) {
    scale *= 10;
    ++length;
  }

  if (length > 7) {
    unsigned long long divisor = length == 8 ? 10 : 100;
    unsigned long long quotient = output / divisor;
    unsigned long long remainder = output % divisor;
    unsigned long long half = divisor / 2;
    bool round_up;
    if (remainder != half) {
      round_up = remainder > half;
    }
    else {
      // A tie in the shortest digits, so the double that Java formats decides: it is the double
      // closest to output * 10^exponent, and Ryu s2d tells whether it is above, below or equal
      long long direction;
      {
        unsigned long long m10 = output;
        int e10_tie = exponent;
        int floor_log2_m10 = 63 - __clzll(m10);
        int tie_e2;
        unsigned long long tie_m2;
        bool trailing_zeros;
        if (e10_tie >= 0) {
          tie_e2 = floor_log2_m10 + e10_tie + log2_pow5(e10_tie) - 53;
          int j = tie_e2 - e10_tie - (log2_pow5(e10_tie) + 1) + 125;
          tie_m2 = mul_shift_64(m10, RYU_POW5_SPLIT[e10_tie], j);
          trailing_zeros = tie_e2 < e10_tie || (tie_e2 - e10_tie < 64 && (m10 & ((1ull << (tie_e2 - e10_tie)) - 1)) == 0);
        }
        else {
          tie_e2 = floor_log2_m10 + e10_tie - (log2_pow5(-e10_tie) + 1) - 53;
          int j = tie_e2 - e10_tie + (log2_pow5(-e10_tie) + 1) - 1 + 125;
          tie_m2 = mul_shift_64(m10, RYU_POW5_INV_SPLIT[-e10_tie], j);
          trailing_zeros = pow5_factor(m10) >= (unsigned int) -e10_tie;
        }
        int ieee_e2 = tie_e2 + 1023 + (63 - __clzll(tie_m2));
        if (ieee_e2 < 0) { ieee_e2 = 0; }
        int shift = (ieee_e2 == 0 ? 1 : ieee_e2) - tie_e2 - 1023 - 52;
        trailing_zeros &= (tie_m2 & ((1ull << (shift - 1)) - 1)) == 0;
        unsigned long long last_removed_bit = (tie_m2 >> (shift - 1)) & 1;
        if (trailing_zeros && last_removed_bit == 0) {
          direction = 0;
        }
        else if (last_removed_bit != 0 && (!trailing_zeros || ((tie_m2 >> shift) & 1) != 0)) {
          direction = 1;
        }
        else {
          direction = -1;
        }
      }
      // JDK 25's DecimalFormat rounds an exact tie up in this range only. JDK 26+ rounds it half even
      // everywhere (https://bugs.openjdk.org/browse/JDK-8362448), so this kernel matches the CPU on JDK 25 only.
      bool half_up = ieee_exponent >= 127 + 23 && ieee_exponent < 127 + 63;
      round_up = direction > 0 || (direction == 0 && (half_up || quotient % 2 == 1));
    }
    output = quotient + round_up;
    exponent += length - 7;
    length = 7;
    scale = 1000000;
    if (output >= 10000000) {
      output /= 10;
      ++exponent;
    }
    while (output % 10 == 0 && length > 1) {
      output /= 10;
      ++exponent;
      --length;
      scale /= 10;
    }
  }

  unsigned long long fraction = output - (output / scale) * scale;
  for (int digit = length - 1; digit < 8; digit++) {
    fraction *= 10;
  }
  int result;
  if (RYU_OUTPUT == 0) {
    result = (int) fraction;
  }
  else {
    result = ((exponent + length - 1 + RYU_EXPONENT_BIAS) << RYU_EXPONENT_SHIFT)
        | ((int) (output / scale) << RYU_LEAD_SHIFT)
        | (length > 1 ? length - 1 : 1);
  }
  *out = result;
}
