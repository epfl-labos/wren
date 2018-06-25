/*
 * WREN 
 *
 * Copyright 2018 Operating Systems Laboratory EPFL
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// __________________________________________________________________
//
// Based on the code of YCSB-C created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
// __________________________________________________________________
//

#ifndef SCC_GENERATORS_H_
#define SCC_GENERATORS_H_

#include <cstdint>
#include <cmath>
#include <cassert>
#include <random>
#include <atomic>
#include <chrono>
#include <algorithm>

#ifdef USE_ASSERT
#define ASSERT(A) assert(A)
#else
#define ASSERT(A)
#endif

namespace scc {

    template<typename Value>
    class Generator {
    public:
        virtual Value Next() = 0;

        virtual Value Last() = 0;

        virtual ~Generator() {
        }
    };

    class UniformGenerator : public Generator<uint64_t> {
    public:
        // Both min and max are inclusive

        UniformGenerator(uint64_t min, uint64_t max) : dist_(min, max) {
            std::array<int, std::mt19937::state_size> seed_data;
            std::random_device r;
            std::generate_n(seed_data.data(), seed_data.size(), std::ref(r));
            std::seed_seq seq(std::begin(seed_data), std::end(seed_data));
            generator_ = new std::mt19937(seq);

            Next();
        }

        UniformGenerator(uint64_t max) : dist_(0, max) {
            std::array<int, std::mt19937::state_size> seed_data;
            std::random_device r;
            std::generate_n(seed_data.data(), seed_data.size(), std::ref(r));
            std::seed_seq seq(std::begin(seed_data), std::end(seed_data));
            generator_ = new std::mt19937(seq);
            Next();
        }

        uint64_t Next() {
            return last_int_ = dist_(*generator_);
        }

        uint64_t Last() {
            return last_int_;
        }

    private:
        uint64_t last_int_;
        std::mt19937 *generator_;
        std::uniform_int_distribution <uint64_t> dist_;
    };

/* Zipfian Distribution Generator*/

    class ZipfianGenerator : public Generator<uint64_t> {
    public:
        constexpr static const double kZipfianConst = 0.99;
        static const uint64_t kMaxNumItems = (UINT64_MAX >> 24);

        ZipfianGenerator(uint64_t min, uint64_t max, double zipfian_const);

        ZipfianGenerator(int id, uint64_t min, uint64_t max, double zipfian_const);

        ZipfianGenerator(int id, uint64_t num_items);

        uint64_t Next(uint64_t num_items);

        uint64_t Next();

        uint64_t Last();

        int tid;

    private:
        ///
        /// Compute the zeta constant needed for the distribution.
        /// Remember the number of items, so if it is changed, we can recompute zeta.
        ///
        void RaiseZeta(uint64_t num);

        double Eta();

        UniformGenerator* _uniformGenerator;

        ///
        /// Calculate the zeta constant needed for a distribution.
        /// Do this incrementally from the last_num of items to the cur_num.
        /// Use the zipfian constant as theta. Remember the new number of items
        /// so that, if it is changed, we can recompute zeta.
        ///
        static double Zeta(uint64_t last_num, uint64_t cur_num, double theta, double last_zeta);

        static double Zeta(uint64_t num, double theta);

        uint64_t num_items_;
        uint64_t base_; /// Min number of items to generate

        // Computed parameters for generating the distribution
        double theta_, zeta_n_, eta_, alpha_, zeta_2_;
        uint64_t n_for_zeta_; /// Number of items used to compute zeta_n
        uint64_t last_value_;
    };

/* Scrambled Zipfian Distribution Generator*/

    class ScrambledZipfianGenerator : public Generator<uint64_t> {
    public:
        ScrambledZipfianGenerator(uint64_t min, uint64_t max, double zipfian_const);

        ScrambledZipfianGenerator(uint64_t num_items);

        uint64_t Next();

        uint64_t Last();

    private:
        uint64_t base_;
        uint64_t num_items_;
        ZipfianGenerator generator_;
        uint64_t last_;
    };

/* Uniform Distribution Generator*/



/* Counter Generator */

    class CounterGenerator : public Generator<uint64_t> {
    public:

        CounterGenerator(uint64_t start) : counter_(start) {
        }

        uint64_t Next();

        uint64_t Last();

        void Set(uint64_t start);

    private:
        std::atomic <uint64_t> counter_;
    };

/* Skewed Latest Generator */


    class SkewedLatestGenerator : public Generator<uint64_t> {
    public:
        SkewedLatestGenerator(int id, CounterGenerator &counter);

        uint64_t Next();

        uint64_t Last();

    private:
        CounterGenerator &basis_;
        ZipfianGenerator zipfian_;
        uint64_t last_;
    };

    class HotSpotGenerator : public Generator<uint64_t> {

    public:
        //HotSpotGenerator(uint64_t hot_min, uint64_t hot_max, uint64_t cold_min, uint64_t cold_max, double hot_probability);
        HotSpotGenerator(uint64_t num_elements, double hot_probability, double hot_percentage);

        uint64_t Next();

        uint64_t Last();

    private:
        UniformGenerator *_hot_generator;
        UniformGenerator *_cold_generator;
        double _hot_probability;
        uint64_t _hot_min;
        uint64_t _hot_max;
        uint64_t _cold_min;
        uint64_t _cold_max;
    };

} // namespace scc

#endif
