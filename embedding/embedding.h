#pragma once

#include <algorithm>
#include <ctime>
#include <numeric>
#include <string>
#include <vector>

#include "common.h"
#include "llama.h"
#include "log.h"

std::string join(const std::vector<std::string>& vec,
                 const std::string& delimiter);

std::vector<std::vector<float>> embedding(const std::string& prompt);

std::vector<float> embedding_single(const std::string& prompt);

std::vector<std::vector<float>> embedding_batch(const std::string& prompts);
