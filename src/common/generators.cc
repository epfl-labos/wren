//
//  zipfian_generator.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/7/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstdint>
#include <cmath>
#include <cassert>
#include <random>
#include "generators.h"

namespace scc {

    // Helper functions
    double RandomDouble(int id,double min = 0.0, double max = 1.0) {
        static std::default_random_engine generator(time(0)*id);
        static std::uniform_real_distribution<double> uniform(min, max);
        return uniform(generator);
    }


    const uint64_t kFNVOffsetBasis64 = 0xCBF29CE484222325;
    const uint64_t kFNVPrime64 = 1099511628211;

    uint64_t FNVHash64(uint64_t val) {
        uint64_t hash = kFNVOffsetBasis64;

        for (int i = 0; i < 8; i++) {
            uint64_t octet = val & 0x00ff;
            val = val >> 8;

            hash = hash ^ octet;
            hash = hash * kFNVPrime64;
        }
        return hash;
    }

    /* Zipfian Distribution Generator*/

    ZipfianGenerator::ZipfianGenerator(uint64_t min, uint64_t max, double zipfian_const = kZipfianConst) :
            num_items_(max - min + 1), base_(min), theta_(zipfian_const), zeta_n_(0), n_for_zeta_(0) {
        ASSERT(num_items_ >= 2 && num_items_ < kMaxNumItems);
        zeta_2_ = Zeta(2, theta_);
        alpha_ = 1.0 / (1.0 - theta_);
        RaiseZeta(num_items_);
        eta_ = Eta();
        _uniformGenerator = new UniformGenerator(0,RAND_MAX-1);
        Next();
    }

    ZipfianGenerator::ZipfianGenerator(int id, uint64_t min, uint64_t max, double zipfian_const = kZipfianConst) :
            tid(id), num_items_(max - min + 1), base_(min), theta_(zipfian_const), zeta_n_(0), n_for_zeta_(0) {
        ASSERT(num_items_ >= 2 && num_items_ < kMaxNumItems);
        zeta_2_ = Zeta(2, theta_);
        alpha_ = 1.0 / (1.0 - theta_);
        RaiseZeta(num_items_);
        eta_ = Eta();
        _uniformGenerator = new UniformGenerator(0,RAND_MAX-1);
        Next();
    }

    ZipfianGenerator::ZipfianGenerator(int id, uint64_t num_items) :
            ZipfianGenerator(id, 0, num_items - 1, kZipfianConst) {
    }

    uint64_t ZipfianGenerator::Next() {
        return Next(num_items_);
    }

    uint64_t ZipfianGenerator::Last() {
        return last_value_;
    }

    void ZipfianGenerator::RaiseZeta(uint64_t num) {
        ASSERT(num >= n_for_zeta_);
        zeta_n_ = Zeta(n_for_zeta_, num, theta_, zeta_n_);
        n_for_zeta_ = num;
    }

    double ZipfianGenerator::Eta() {
        return (1 - std::pow(2.0 / num_items_, 1 - theta_)) /
               (1 - zeta_2_ / zeta_n_);
    }

    ///
    /// Calculate the zeta constant needed for a distribution.
    /// Do this incrementally from the last_num of items to the cur_num.
    /// Use the zipfian constant as theta. Remember the new number of items
    /// so that, if it is changed, we can recompute zeta.
    ///

    double ZipfianGenerator::Zeta(uint64_t last_num, uint64_t cur_num,
                                  double theta, double last_zeta) {
        double zeta = last_zeta;
        for (uint64_t i = last_num + 1; i <= cur_num; ++i) {
            zeta += 1 / std::pow(i, theta);
        }
        return zeta;
    }

    double ZipfianGenerator::Zeta(uint64_t num, double theta) {
        return Zeta(0, num, theta, 0);
    }

    uint64_t ZipfianGenerator::Next(uint64_t num) {
        ASSERT(num >= 2 && num < kMaxNumItems);
        if (num > n_for_zeta_) { // Recompute zeta_n and eta
            RaiseZeta(num);
            eta_ = Eta();
        }

        double u =  ((double)_uniformGenerator->Next())/ (RAND_MAX);// RandomDouble(tid);
        double uz = u * zeta_n_;

        if (uz < 1.0) {
            return last_value_ = 0;
        }

        if (uz < 1.0 + std::pow(0.5, theta_)) {
            return last_value_ = 1;
        }

        return last_value_ = base_ + num * std::pow(eta_ * u - eta_ + 1, alpha_);
    }

    /* Scrambled Zipfian Distribution Generator*/

    ScrambledZipfianGenerator::ScrambledZipfianGenerator(uint64_t min, uint64_t max,
                                                         double zipfian_const = ZipfianGenerator::kZipfianConst) :
            base_(min), num_items_(max - min + 1),
            generator_(min, max, zipfian_const) {
    }

    ScrambledZipfianGenerator::ScrambledZipfianGenerator(uint64_t num_items) :
            ScrambledZipfianGenerator(0, num_items - 1) {
    }

    uint64_t ScrambledZipfianGenerator::Next() {
        uint64_t value = generator_.Next();
        value = base_ + FNVHash64(value) % num_items_;
        return last_ = value;
    }

    uint64_t ScrambledZipfianGenerator::Last() {
        return last_;
    }

    /* Counter Generator */

    uint64_t CounterGenerator::Next() {
        return counter_.fetch_add(1);
    }

    uint64_t CounterGenerator::Last() {
        return counter_.load() - 1;
    }

    void CounterGenerator::Set(uint64_t start) {
        counter_.store(start);
    }

    /* Skewed Latest Generator */

    SkewedLatestGenerator::SkewedLatestGenerator(int id,CounterGenerator &counter) :
            basis_(counter), zipfian_(id, basis_.Last()) {
        Next();
    }

    uint64_t SkewedLatestGenerator::Next() {
        uint64_t max = basis_.Last();
        return last_ = max - zipfian_.Next(max);
    }

    uint64_t SkewedLatestGenerator::Last() {
        return last_;
    }

    int main() {

        ZipfianGenerator generator(3, 0, 1000000, 0.9);

        double numbers[1000000];

        for (int i = 0; i < 1000000; i++) {
            numbers[i] = 0;
        }

        for (int i = 0; i < 1000000000; i++) {
            numbers[generator.Next()]++;
        }

        for (int i = 0; i < 1000000; i++) {
            numbers[i] /= 1000000000;
        }

        for (int i = 0; i < 20; i++) {
            fprintf(stdout, "%lf ", numbers[i]);
        }

        return 0;
    }

/*
    HotSpotGenerator::HotSpotGenerator(uint64_t hot_min, uint64_t hot_max, uint64_t cold_min, uint64_t cold_max,
                                       double hot_probability) : _hot_min(hot_min), _hot_max(hot_max),
                                                                 _cold_min(cold_min), _cold_max(cold_max),
                                                                 _hot_probability(hot_probability),
                                                                 _cold_generator(_cold_min, _cold_max),
                                                                 _hot_generator(_hot_min, _hot_max) {
    }*/

    HotSpotGenerator::HotSpotGenerator(uint64_t num,
                                       double hot_probability,
                                       double hot_percentage) : _hot_min(0),
                                                                _hot_max(hot_percentage * num),
                                                                _cold_min(_hot_max + 1),
                                                                _cold_max(num - 1),
                                                                _hot_probability(hot_probability)
                                                                {
        _hot_min = 0;
        _hot_max = (hot_percentage * num);
        _cold_min = _hot_max + 1;
        _cold_max = num - 1;
        fprintf(stdout, "Hot min %d Hot max %d Cold min %d Cold max %d hot prob %f\n", _hot_min, _hot_max, _cold_min,
                _cold_max);
        fflush(stdout);

        _cold_generator = new UniformGenerator(_cold_min, _cold_max);
        _hot_generator = new UniformGenerator(_hot_min, _hot_max);

    }

    uint64_t HotSpotGenerator::Next() {
        if (RandomDouble(0, 1.0) < _hot_probability) {
            return _hot_generator->Next();
        }
        return _cold_generator->Next();
    }

    uint64_t HotSpotGenerator::Last() {
        0;
    }


}