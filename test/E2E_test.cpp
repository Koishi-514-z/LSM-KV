#include "../test.h"
#include <fstream>
#include <iostream>
#include <string>

#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>
#include <fstream>


std::vector<std::string> read_file(std::string filename) {
	// read file from ./data/trimmed_text
	// every line is a string
	// skip all the lines that are empty
	std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr<<"Failed to open file: "<<filename<<std::endl;
        return {};
    }
    std::string line;
    std::vector<std::string> temp;
    while (std::getline(file, line)) {
        //std::cout<<line<<std::endl;
        // check if all characters in the line cannot be seen
        // 检查是不是全是不可见字符
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
        if(line.size() < 30) {
            continue;
        }
        // std::cout<<line<<std::endl;
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
        max = std::min(max, (uint64_t)trimmed_text.size());
        for (i = 0; i < max; ++i) {
            store.put(i, trimmed_text[i]);
        }

        for (i = 0; i < max; ++i)
            EXPECT(trimmed_text[i], store.get(i));
        // phase();
        
        // run the search_knn, and compare the result to ./data/test_text_ans.txt
        auto test_text = read_file("./data/test_text.txt");
        max = std::min(max, (uint64_t)test_text.size());

        std::vector<std::string> ans;
        std::ifstream file("./data/test_text_ans.txt");
        if (!file.is_open()) {
            std::cerr << "Failed to open the file" << std::endl;
            return;
        }
        std::string line;
        while (std::getline(file, line)) {
            ans.push_back(line);
        }
        file.close();
        int idx = 0;
        for (i = 0; i < max; ++i) {
            auto res = store.search_knn(test_text[i], 5);
            for(auto j : res) {
                EXPECT(ans[idx], j.second);
                EXPECT(store.get(j.first), j.second);
                idx++;
            }
        }
        phase();

    }

public:
    CorrectnessTest(const std::string &dir, bool v = true) : Test(dir, v) {}

    void start_test(void *args = NULL) override {
        std::cout << "KVStore Correctness Test" << std::endl;

        // store.reset();

        // std::cout << "[Simple Test]" << std::endl;
        // regular_test(SIMPLE_TEST_MAX);

        // store.reset();

        // std::cout << "[Middle Test]" << std::endl;
        // regular_test(MIDDLE_TEST_MAX);

        // store.reset();
        std::cout << "[Text Test]" << std::endl;
        // regular_test(LARGE_TEST_MAX);
        text_test(80);

        //        store.reset();
        //        std::cout << "[Insert Test]" << std::endl;
        //        insert_test(1024 * 16);

        //        store.reset();
        //        std::cout << "[delete test]" << std::endl;
        //        delete_test(1024 * 64);
    }
};

int main(int argc, char *argv[]) {
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