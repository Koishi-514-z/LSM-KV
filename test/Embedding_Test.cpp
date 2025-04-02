#include <fstream>
#include <iostream>
#include <map>

#include "embedding.h"

int basic() {
  std::vector<std::string> words{"Apple", "Banana", "Orange",
                                 "Watch", "Man",    "Chicken"};
  std::string prompt;
  std::string joined = join(words, "\n");
  std::vector<std::vector<float>> vec = embedding(joined);
  if (vec.empty()) {
    std::cerr << "Failed to embed the prompt" << std::endl;
    return 1;
  }

  size_t n_embd = vec[0].size();
  size_t n_prompts = vec.size();
  std::map<std::string, std::map<std::string, float>> sim_matrix;
  for (int i = 0; i < n_prompts; i++) {
    for (int j = 0; j < n_prompts; j++) {
      float sim =
          common_embd_similarity_cos(vec[i].data(), vec[j].data(), n_embd);
      sim_matrix[words[i]][words[j]] = sim;
    }
  }

  int passed_count = 0;

  if (sim_matrix["Apple"]["Banana"] > sim_matrix["Apple"]["Man"]) {
    passed_count++;
  }
  if (sim_matrix["Apple"]["Orange"] > sim_matrix["Apple"]["Chicken"]) {
    passed_count++;
  }
  if (sim_matrix["Banana"]["Orange"] > sim_matrix["Banana"]["Man"]) {
    passed_count++;
  }

  return passed_count;
}

int large() {
  int passed_count = 0;
  const int max_size = 1024 * 128;
  int read_size = 0;
  std::ifstream file("./data/trimmed_text.txt");
  if (!file.is_open()) {
    std::cerr << "Failed to open the file" << std::endl;
    return passed_count;
  }
  std::string line;
  std::string prompt;
  std::vector<std::string> sentences;
  while (std::getline(file, line)) {
    if (line.empty()) continue;
    prompt += line + "\n";
    sentences.push_back(line);
    read_size += line.size();
    if (read_size > max_size) {
      break;
    }
  }
  prompt.pop_back();
  std::vector<std::vector<float>> vec = embedding(prompt);
  if (vec.empty()) {
    std::cerr << "Failed to embed the prompt" << std::endl;
    return passed_count;
  }
  passed_count++;
  const std::string chat_prompt =
      "What did Ward do and what did he won in Michigan High School?";
  std::vector<std::vector<float>> chat_vec = embedding(chat_prompt);
  if (chat_vec.empty()) {
    std::cerr << "Failed to embed the chat prompt" << std::endl;
    return passed_count;
  }
  passed_count++;

  float max_sim = 0;
  std::string max_sim_sentence;
  size_t n_embd = vec[0].size();
  size_t n_prompts = vec.size();

  for (int i = 0; i < n_prompts; i++) {
    float sim =
        common_embd_similarity_cos(vec[i].data(), chat_vec[0].data(), n_embd);
    if (sim > max_sim) {
      max_sim = sim;
      max_sim_sentence = sentences[i];
    }
  }

  std::string supposed_sentence =
      "Ward was the Michigan High School Athlete of the Year, after setting a "
      "national prep record in the high jump. At the University of Michigan, "
      "he was a collegiate champion in the high jump, the long jump, the "
      "100-yard dash, and the 440-yard dash, and finished second in the voting "
      "for the Associated Press Big Ten Athlete of the Year award in 1933. In "
      "track and field he was a three-time All-American and eight-time Big Ten "
      "champion.";
  if (max_sim_sentence != supposed_sentence) {
    std::cerr << "Failed to find the correct sentence" << std::endl;
    return passed_count;
  }
  passed_count++;
  return passed_count;
}

int main() {
  int passed_count = 0;
  const int max_grade = 6;
  passed_count += basic();
  passed_count += large();
  if (passed_count != max_grade) {
    std::cerr << "Failed to pass all tests" << std::endl;
  }
  if (passed_count == max_grade) {
    std::cout << "All tests passed" << std::endl;
  }
  return 0;
}
