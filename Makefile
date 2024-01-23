BIN_DIR := bin
TOOLS_DIR := target/tools
TOOLS_BIN_DIR := $(abspath $(TOOLS_DIR)/$(BIN_DIR))

$(TOOLS_BIN_DIR):
	mkdir -p $(TOOLS_BIN_DIR)

export PATH := $(abspath $(TOOLS_BIN_DIR)):$(PATH)

.PHONY: test
test: $(TOOLS_BIN_DIR)
	./scripts/install-kwok.sh $(TOOLS_BIN_DIR)
	cargo t --features archive
