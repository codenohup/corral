BUCKET = ${AWS_TEST_BUCKET}
BIN_DIR = ./bin
PROG_NAME = word_count

.PHONY: all clean $(PROG_NAME) input_in_s3

all: $(PROG_NAME)

$(PROG_NAME):
	go build -o $(BIN_DIR)/$@ .

input_in_s3:
	aws s3 cp ./metamorphosis.txt s3://${BUCKET}

test_wc_local: $(PROG_NAME) delete
	#$(BIN_DIR)/$(PROG_NAME) --verbose --cleanup=false metamorphosis.txt
	#$(BIN_DIR)/$(PROG_NAME) -c-verbose --cleanup=false /Users/chase/Downloads/wordcount_dataset/
	$(BIN_DIR)/$(PROG_NAME) --verbose --cleanup=false /Users/chase/Downloads/wordcount_dataset/input_0.txt
	#$(BIN_DIR)/$(PROG_NAME) --verbose --cleanup=false /Users/chase/Downloads/wordcount_dataset_10GB/input_92.txt

test_wc_s3: $(PROG_NAME) input_in_s3
	$(BIN_DIR)/$(PROG_NAME) --out s3://${BUCKET}/ s3://${BUCKET}/metamorphosis.txt

#test_wc_lambda: $(PROG_NAME) input_in_s3
#	$(BIN_DIR)/$(PROG_NAME) --lambda --out s3://${BUCKET}/ s3://${BUCKET}/metamorphosis.txt

test_wc_lambda: $(PROG_NAME) delete_s3
	#$(BIN_DIR)/$(PROG_NAME) --lambda --verbose --cleanup=false --out s3://huangxu/wordcount/workspace/ --localout examples/word_count/ s3://huangxu/datasets/1GB/wordcount_dataset/input_0.txt
	#$(BIN_DIR)/$(PROG_NAME) --lambda --verbose --cleanup=false --out s3://huangxu/wordcount/workspace/ --localout examples/word_count/ s3://huangxu/wordcount/input/
	#$(BIN_DIR)/$(PROG_NAME) --lambda --verbose --cleanup=false --out s3://huangxu/wordcount/workspace/ --localout examples/word_count/ s3://huangxu/datasets/word_count_10GB/wordcount_dataset_10GB/input_0.txt
	$(BIN_DIR)/$(PROG_NAME) --lambda --verbose --cleanup=false --out s3://huangxu/wordcount/workspace/ --localout examples/word_count/ s3://huangxu/datasets/word_count_10GB/wordcount_dataset_10GB/

clean:
	find . -name "*.out" -print0 | xargs -0 rm
	rm -f $(BIN_DIR)/$(PROG_NAME) output*
	aws s3 rm s3://${BUCKET} --recursive

delete_s3:
	aws s3 rm --recursive s3://huangxu/wordcount/workspace/

delete:
	rm -rf ./Shuffle_merge ./Shuffle_origin ./Output ./output*	./Shuffle_combine