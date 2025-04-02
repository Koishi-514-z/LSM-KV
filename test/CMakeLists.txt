add_executable(Embedding_Test Embedding_Test.cpp)

add_executable(E2E_test E2E_test.cpp)

target_link_libraries(Embedding_Test PUBLIC embedding)

target_link_libraries(E2E_test PUBLIC kvstore)
