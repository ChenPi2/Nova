# Makefile - build the Reactor TCP server
#
#   make          编译
#   make clean    清理
#   make run      编译并运行（端口 9100）

CXX      := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -O2 -pthread
TARGET   := server
SRC      := server.cpp

.PHONY: all clean run

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) $(SRC) -o $(TARGET)

run: $(TARGET)
	./$(TARGET) 9100

clean:
	rm -f $(TARGET)
