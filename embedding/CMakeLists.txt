add_library(embedding STATIC embedding.cc)

target_link_libraries(embedding PUBLIC llama common)

target_include_directories(embedding PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
