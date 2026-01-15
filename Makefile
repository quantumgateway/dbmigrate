BINARY_NAME=dbmigrate
INSTALL_DIR=$(HOME)/go/bin

.PHONY: build install clean

build:
	go build -o $(BINARY_NAME) .

install: build
	mkdir -p $(INSTALL_DIR)
	cp $(BINARY_NAME) $(INSTALL_DIR)/

clean:
	rm -f $(BINARY_NAME)
