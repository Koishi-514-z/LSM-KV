#include "../test.h"
#include <fstream>
#include <iostream>
#include <string>

#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>
#include <fstream>

std::ofstream error_log;


std::vector<std::string> read_file(std::string filename) {
	std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr<<"Failed to open file: "<<filename<<std::endl;
        return {};
    }
    std::string line;
    std::vector<std::string> temp;
    while (std::getline(file, line)) {
        bool exist_alpha = false;
        for (auto c : line) {
            if (isalpha(c)) {
                exist_alpha = true;
                break;
            }
        }
        if (!exist_alpha) {
            continue;
        }
        if (line.empty())
            continue;
        if(line.size() < 70) {
            continue;
        }
        temp.push_back(line);
    }
    file.close();
    return temp;
}

class CorrectnessTest : public Test {
private:
    const uint64_t SIMPLE_TEST_MAX = 512;
    const uint64_t MIDDLE_TEST_MAX  = 1024 * 64;
    const uint64_t LARGE_TEST_MAX  = 1024 * 64;

	void text_test(uint64_t max) {
		uint64_t i;
		auto trimmed_text = read_file("./data/trimmed_text.txt");
		max				  = std::min(max, (uint64_t)trimmed_text.size());
		for (i = 0; i < max; ++i) {
			store.put(i, trimmed_text[i]);
		}

		for (i = 0; i < max; ++i)
			EXPECT(trimmed_text[i], store.get(i));
		// phase();

		// run the search_knn, and compare the result to ./data/test_text_ans.txt
		auto test_text = read_file("./data/test_text.txt");
		max			   = std::min(max, (uint64_t)test_text.size());

		std::vector<std::string> ans;
        ans = read_file("./data/test_text_ans.txt");
        phase();
		int idx = 0, k = 3;
		for (i = 0; i < max; ++i) {
			auto res = store.search_knn(test_text[i], k);
			for (auto j : res) {
                if (ans[idx] != j.second) {
                    error_log << "\nMISMATCH at idx=" << idx << std::endl;
                    error_log << "Expected: " << ans[idx].substr(0, 100) << "..." << std::endl;
                    error_log << "Actual: " << j.second.substr(0, 100) << "..." << std::endl;
                }
                if(store.get(j.first) != j.second) {
                    std::cerr << "TEST Error @" << __FILE__ << ":" << __LINE__;
                    std::cerr << ", expected " << ans[idx];
                    std::cerr << ", got " << j.second << std::endl;
                }
				EXPECT(ans[idx], j.second);
				idx++;
			}
		}
		auto phase_with_tolerance = [this](double tolerance = 0.03) {
			// Report
			std::cout << "  Phase " << (nr_phases + 1) << ": ";
			std::cout << nr_passed_tests << "/" << nr_tests << " ";

			// Calculate tolerance
			double pass_rate		   = static_cast<double>(nr_passed_tests) / nr_tests;
			bool passed_with_tolerance = pass_rate >= (1.0 - tolerance);

			// Count
			++nr_phases;
			if (passed_with_tolerance) {
				++nr_passed_phases;
				std::cout << "[PASS]" << std::endl;
			} else {
                std::cout << "Accept Rate: " << pass_rate * 100 << "%\n";
                std::cout << "The Accept Rate we recommend is more than 85%.\nBecause the embedding model may not act strictly samely between each machine.\n";
			}

			std::cout.flush();

			// Reset
			nr_tests		= 0;
			nr_passed_tests = 0;
		};
		phase_with_tolerance(0.15);
	}

public:
    CorrectnessTest(const std::string &dir, bool v = true) : Test(dir, v) {}

    void start_test(void *args = NULL) override {
        std::cout << "===========================" << std::endl;
        std::cout << "KVStore Correctness Test" << std::endl;
        
        store.reset();
        std::cout << "[Text Test]" << std::endl;
        text_test(120);
    }
};

int main(int argc, char *argv[]) {
    error_log.open("knn_errors.log");
    if (!error_log.is_open()) {
        std::cerr << "Failed to open error log file" << std::endl;
    }

    bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

    std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
    std::cout << "  -v: print extra info for failed tests [currently ";
    std::cout << (verbose ? "ON" : "OFF") << "]" << std::endl;
    std::cout << std::endl;
    std::cout.flush();

    CorrectnessTest test("./data", verbose);

    test.start_test();

    return 0;
}