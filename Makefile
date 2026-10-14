CC      = gcc
CMAKE   = cmake
CFLAGS  = -Wall -Wextra -Wpedantic -std=c11 -D_GNU_SOURCE -Iinclude -O2
LDFLAGS = -pthread
RS_LDFLAGS = -lerasurecode_rs_vand -lstdc++ -lm

CB_DIR   = ../buffer-management-module
CB_INC   = $(CB_DIR)/include
CB_SRC   = $(CB_DIR)/src/circular_buffer.c

SRC_DIR  = src
TEST_DIR = tests
WG_DIR    = apps/wg_multi_pipeline
OBJ_DIR  = build
RSCODE_DIR = third_party/rscode
WIREHAIR_DIR = third_party/wirehair
WIREHAIR_BUILD_DIR = $(OBJ_DIR)/wirehair
WIREHAIR_LIB = $(WIREHAIR_BUILD_DIR)/libwirehair.a
WIREHAIR_SRCS := $(wildcard $(WIREHAIR_DIR)/*.cpp) \
	$(wildcard $(WIREHAIR_DIR)/*.h) \
	$(wildcard $(WIREHAIR_DIR)/codec/*.cpp) \
	$(wildcard $(WIREHAIR_DIR)/codec/*.h) \
	$(WIREHAIR_DIR)/include/wirehair/wirehair.h \
	$(WIREHAIR_DIR)/CMakeLists.txt

CFLAGS += -I$(CB_INC)

LIB_SRCS = \
	$(SRC_DIR)/packet.c \
	$(SRC_DIR)/packet_pool.c \
	$(SRC_DIR)/time_utils.c \
	$(SRC_DIR)/flow_buffer.c \
	$(SRC_DIR)/mixed_queue.c \
	$(SRC_DIR)/fd_sink.c \
	$(SRC_DIR)/flow_context.c \
	$(SRC_DIR)/flow_worker.c \
	$(SRC_DIR)/dispatcher.c \
	$(SRC_DIR)/flow_manager.c \
	$(SRC_DIR)/pipe_io.c \
	$(SRC_DIR)/flow_peer_map.c \
	$(SRC_DIR)/ingress_push.c \
	$(SRC_DIR)/wire_header.c \
	$(SRC_DIR)/tx_queue.c \
	$(SRC_DIR)/fec_transport.c

LIB_OBJS = $(LIB_SRCS:$(SRC_DIR)/%.c=$(OBJ_DIR)/%.o) \
           $(OBJ_DIR)/circular_buffer.o

LIB      = $(OBJ_DIR)/libmulti_flow.a
TEST_BIN = $(OBJ_DIR)/run_tests
WG_BIN    = $(OBJ_DIR)/wg_multi_pipeline
RELAY_DIR = apps/wire_relay
RELAY_BIN = $(OBJ_DIR)/wire_relay
WG_CODEC_TEST_BIN = $(OBJ_DIR)/wg_codec_tests
FEC_TRACE_BIN = $(OBJ_DIR)/fec_trace
RS_RECOVERY_BENCH_BIN = $(OBJ_DIR)/rs_recovery_bench
RS_ENCODE_BENCH_BIN = $(OBJ_DIR)/rs_encode_bench
RS_ENCODE_FAST_TEST_BIN = $(OBJ_DIR)/rs_encode_fast_tests
RS_ENCODE_GENERAL_TEST_BIN = $(OBJ_DIR)/rs_encode_general_tests

WG_APP_SRCS = \
	$(WG_DIR)/main.c \
	$(WG_DIR)/pipeline.c \
	$(WG_DIR)/buffer_transfer.c \
	$(WG_DIR)/codec.c \
	$(WG_DIR)/block_codec.c \
	$(WG_DIR)/copy_codec.c \
	$(WG_DIR)/xor_fec_codec.c \
	$(WG_DIR)/rs_fec_codec.c \
	$(WG_DIR)/rs_codec.c \
	$(WG_DIR)/wirehair_segment.c \
	$(WG_DIR)/wire_udp.c \
	$(WG_DIR)/file_drain.c

RSCODE_SRCS = \
	$(RSCODE_DIR)/rs.c \
	$(RSCODE_DIR)/galois.c \
	$(RSCODE_DIR)/berlekamp.c \
	$(RSCODE_DIR)/crcgen.c

RSCODE_OBJS = \
	$(OBJ_DIR)/rscode_rs.o \
	$(OBJ_DIR)/rscode_galois.o \
	$(OBJ_DIR)/rscode_berlekamp.o \
	$(OBJ_DIR)/rscode_crcgen.o \
	$(OBJ_DIR)/wg_rs_gf256_simd.o \
	$(OBJ_DIR)/wg_rs_gf256_avx2.o \
	$(OBJ_DIR)/wg_rs_gf256_ssse3.o \
	$(OBJ_DIR)/wg_wirehair_segment.o \
	$(OBJ_DIR)/wg_wirehair_segment_sender.o \
	$(WIREHAIR_LIB)

HOST_ARCH := $(shell uname -m)
ifneq ($(filter x86_64 amd64 i386 i686,$(HOST_ARCH)),)
RS_AVX2_CFLAGS = -mavx2
RS_SSSE3_CFLAGS = -mssse3
endif

WG_OBJS = \
	$(OBJ_DIR)/wg_main.o \
	$(OBJ_DIR)/wg_pipeline.o \
	$(OBJ_DIR)/wg_buffer_transfer.o \
	$(OBJ_DIR)/wg_codec.o \
	$(OBJ_DIR)/wg_block_codec.o \
	$(OBJ_DIR)/wg_copy_codec.o \
	$(OBJ_DIR)/wg_xor_fec_codec.o \
	$(OBJ_DIR)/wg_rs_fec_codec.o \
	$(OBJ_DIR)/wg_rs_codec.o \
	$(OBJ_DIR)/wg_wire_udp.o \
	$(OBJ_DIR)/wg_wire_flow_decoder.o \
	$(OBJ_DIR)/wg_file_drain.o

WG_TEST_IN0  = $(OBJ_DIR)/wg_test_in0.ts
WG_TEST_IN1  = $(OBJ_DIR)/wg_test_in1.ts
WG_TEST_IN2  = $(OBJ_DIR)/wg_test_in2.ts
WG_TEST_OUT0 = $(OBJ_DIR)/wg_test_out0.ts
WG_TEST_OUT1 = $(OBJ_DIR)/wg_test_out1.ts
WG_TEST_OUT2 = $(OBJ_DIR)/wg_test_out2.ts

RELAY_OBJS = \
	$(OBJ_DIR)/relay_main.o \
	$(OBJ_DIR)/relay_relay.o \
	$(OBJ_DIR)/relay_recode.o \
	$(OBJ_DIR)/relay_rs_bats_recoder.o \
	$(OBJ_DIR)/relay_egress_queue.o \
	$(OBJ_DIR)/relay_datagram_pool.o \
	$(OBJ_DIR)/relay_deferred.o \
	$(OBJ_DIR)/relay_generation_cache.o \
	$(OBJ_DIR)/relay_local_decode.o \
	$(OBJ_DIR)/relay_local_source.o

RELAY_CORE_OBJS = \
	$(OBJ_DIR)/relay_relay.o \
	$(OBJ_DIR)/relay_recode.o \
	$(OBJ_DIR)/relay_rs_bats_recoder.o \
	$(OBJ_DIR)/relay_egress_queue.o \
	$(OBJ_DIR)/relay_datagram_pool.o \
	$(OBJ_DIR)/relay_deferred.o \
	$(OBJ_DIR)/relay_generation_cache.o

RELAY_LIB_OBJS = \
	$(RELAY_CORE_OBJS) \
	$(OBJ_DIR)/relay_local_decode.o \
	$(OBJ_DIR)/relay_local_source.o

RELAY_DECODE_OBJS = \
	$(OBJ_DIR)/wg_wire_flow_decoder.o \
	$(OBJ_DIR)/wg_codec.o \
	$(OBJ_DIR)/wg_block_codec.o \
	$(OBJ_DIR)/wg_copy_codec.o \
	$(OBJ_DIR)/wg_xor_fec_codec.o \
	$(OBJ_DIR)/wg_rs_fec_codec.o \
	$(OBJ_DIR)/wg_rs_codec.o \
	$(RSCODE_OBJS)

FEC_TRANSPORT_TEST_BIN = $(OBJ_DIR)/fec_transport_tests
FEC_INTERLEAVE_SIM_BIN = $(OBJ_DIR)/fec_interleave_sim
RELAY_GEN_CACHE_TEST_BIN = $(OBJ_DIR)/relay_gen_cache_tests
RELAY_EGRESS_QUEUE_TEST_BIN = $(OBJ_DIR)/relay_egress_queue_tests
RELAY_DEFERRED_TEST_BIN = $(OBJ_DIR)/relay_deferred_tests
RELAY_LOCAL_DECODE_TEST_BIN = $(OBJ_DIR)/relay_local_decode_tests
RELAY_LOCAL_SOURCE_TEST_BIN = $(OBJ_DIR)/relay_local_source_tests
RS_BATS_RECODER_TEST_BIN = $(OBJ_DIR)/rs_bats_recoder_tests
WIRE_UDP_RECV_DEMUX_TEST_BIN = $(OBJ_DIR)/wire_udp_recv_demux_tests
WIRE_FLOW_DECODER_RS_TEST_BIN = $(OBJ_DIR)/wire_flow_decoder_rs_tests
WIRE_FLOW_DECODER_SYS_TEST_BIN = $(OBJ_DIR)/wire_flow_decoder_systematic_tests
WIRE_FLOW_DECODER_REORDER_TEST_BIN = $(OBJ_DIR)/wire_flow_decoder_reorder_tests
WIREHAIR_SEGMENT_TEST_BIN = $(OBJ_DIR)/wirehair_segment_tests

RELAY_HDRS := $(wildcard $(RELAY_DIR)/*.h)

BATS_DIR = ../bats
BATS_LINE_BIN = $(OBJ_DIR)/bats_line
BATS_LINE_SRCS = \
	apps/bats_line/bats_line.c \
	$(BATS_DIR)/field.c \
	$(BATS_DIR)/source.c \
	$(BATS_DIR)/dest.c \
	$(BATS_DIR)/codec.c \
	$(BATS_DIR)/sim.c \
	$(BATS_DIR)/link.c \
	$(BATS_DIR)/relay.c

.PHONY: all test check wg-demo wire-relay wire-relay-hol-baseline bats-line integration-test fec-transport fec-interleave-sim fec-trace rs-recovery-bench rs-encode-bench sanitize tsan clean

all: $(LIB)

wg-demo: $(WG_BIN)

wire-relay: $(RELAY_BIN)

bats-line: $(BATS_LINE_BIN)

$(BATS_LINE_BIN): $(BATS_LINE_SRCS) | $(OBJ_DIR)
	$(CC) -std=c11 -Wall -Wextra -O3 -I$(BATS_DIR) -I../buffer-management-module/include -o $@ $(BATS_LINE_SRCS) ../buffer-management-module/src/circular_buffer.c -pthread -lm

# TEST ONLY: Phase-0 HOL baseline (inline RX on the recv thread).
wire-relay-hol-baseline: $(OBJ_DIR)/wire_relay_hol_baseline

$(OBJ_DIR)/relay_relay_inline.o: $(RELAY_DIR)/relay.c $(INCLUDE_HDRS) $(RELAY_HDRS) $(WG_HDRS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -DRELAY_TEST_INLINE_RX=1 -I$(RELAY_DIR) -I$(WG_DIR) -c $< -o $@

$(OBJ_DIR)/wire_relay_hol_baseline: $(OBJ_DIR)/relay_main.o $(OBJ_DIR)/relay_relay_inline.o \
	$(OBJ_DIR)/relay_recode.o $(OBJ_DIR)/relay_egress_queue.o $(OBJ_DIR)/relay_datagram_pool.o \
	$(OBJ_DIR)/relay_deferred.o \
	$(OBJ_DIR)/relay_generation_cache.o $(OBJ_DIR)/relay_local_decode.o \
	$(OBJ_DIR)/relay_local_source.o \
	$(OBJ_DIR)/wire_header.o $(RELAY_DECODE_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(RELAY_DIR) -I$(WG_DIR) $^ -o $@ $(LDFLAGS) $(RS_LDFLAGS)

fec-trace: $(FEC_TRACE_BIN)
	./$(FEC_TRACE_BIN)

rs-recovery-bench: $(RS_RECOVERY_BENCH_BIN)
	./$(RS_RECOVERY_BENCH_BIN)

rs-encode-bench: $(RS_ENCODE_BENCH_BIN)
	./$(RS_ENCODE_BENCH_BIN)

fec-transport: $(FEC_TRANSPORT_TEST_BIN)
	./$(FEC_TRANSPORT_TEST_BIN)

fec-interleave-sim: $(FEC_INTERLEAVE_SIM_BIN)
	./$(FEC_INTERLEAVE_SIM_BIN)

test check: $(TEST_BIN) $(RELAY_GEN_CACHE_TEST_BIN) $(RELAY_EGRESS_QUEUE_TEST_BIN) \
	$(RELAY_DEFERRED_TEST_BIN) \
	$(RELAY_LOCAL_DECODE_TEST_BIN) \
	$(RELAY_LOCAL_SOURCE_TEST_BIN) \
	$(RS_BATS_RECODER_TEST_BIN) \
	$(WIRE_UDP_RECV_DEMUX_TEST_BIN) $(WIRE_FLOW_DECODER_RS_TEST_BIN) \
	$(WIRE_FLOW_DECODER_SYS_TEST_BIN) $(WIRE_FLOW_DECODER_REORDER_TEST_BIN) \
	$(RS_ENCODE_FAST_TEST_BIN) \
	$(RS_ENCODE_GENERAL_TEST_BIN) \
	$(WIREHAIR_SEGMENT_TEST_BIN) \
	$(FEC_TRANSPORT_TEST_BIN) \
	$(FEC_INTERLEAVE_SIM_BIN)
	./$(TEST_BIN)
	./$(RELAY_GEN_CACHE_TEST_BIN)
	./$(RELAY_EGRESS_QUEUE_TEST_BIN)
	./$(RELAY_DEFERRED_TEST_BIN)
	./$(RELAY_LOCAL_DECODE_TEST_BIN)
	./$(RELAY_LOCAL_SOURCE_TEST_BIN)
	./$(RS_BATS_RECODER_TEST_BIN)
	./$(WIRE_UDP_RECV_DEMUX_TEST_BIN)
	./$(WIRE_FLOW_DECODER_RS_TEST_BIN)
	./$(WIRE_FLOW_DECODER_SYS_TEST_BIN)
	./$(WIRE_FLOW_DECODER_REORDER_TEST_BIN)
	./$(RS_ENCODE_FAST_TEST_BIN)
	./$(RS_ENCODE_GENERAL_TEST_BIN)
	./$(WIREHAIR_SEGMENT_TEST_BIN)
	./$(FEC_TRANSPORT_TEST_BIN)
	./$(FEC_INTERLEAVE_SIM_BIN)

integration-test wg-demo-test: $(WG_BIN) $(RELAY_BIN) $(WG_CODEC_TEST_BIN) $(RELAY_GEN_CACHE_TEST_BIN) \
	$(RELAY_EGRESS_QUEUE_TEST_BIN) $(RELAY_DEFERRED_TEST_BIN) $(RELAY_LOCAL_DECODE_TEST_BIN) \
	$(RELAY_LOCAL_SOURCE_TEST_BIN) $(RS_BATS_RECODER_TEST_BIN) \
	$(WIRE_UDP_RECV_DEMUX_TEST_BIN) $(WIRE_FLOW_DECODER_RS_TEST_BIN) \
	$(WIRE_FLOW_DECODER_SYS_TEST_BIN) $(WIRE_FLOW_DECODER_REORDER_TEST_BIN) \
	$(RS_ENCODE_FAST_TEST_BIN) \
	$(RS_ENCODE_GENERAL_TEST_BIN) \
	$(WIREHAIR_SEGMENT_TEST_BIN) \
	$(FEC_TRANSPORT_TEST_BIN) \
	$(FEC_INTERLEAVE_SIM_BIN)
	./$(WG_CODEC_TEST_BIN)
	./$(RS_ENCODE_FAST_TEST_BIN)
	./$(RS_ENCODE_GENERAL_TEST_BIN)
	./$(WIREHAIR_SEGMENT_TEST_BIN)
	./$(FEC_TRANSPORT_TEST_BIN)
	./$(FEC_INTERLEAVE_SIM_BIN)
	./$(RELAY_GEN_CACHE_TEST_BIN)
	./$(RELAY_EGRESS_QUEUE_TEST_BIN)
	./$(RELAY_DEFERRED_TEST_BIN)
	./$(RELAY_LOCAL_DECODE_TEST_BIN)
	./$(RELAY_LOCAL_SOURCE_TEST_BIN)
	./$(RS_BATS_RECODER_TEST_BIN)
	./$(WIRE_UDP_RECV_DEMUX_TEST_BIN)
	./$(WIRE_FLOW_DECODER_RS_TEST_BIN)
	./$(WIRE_FLOW_DECODER_SYS_TEST_BIN)
	./$(WIRE_FLOW_DECODER_REORDER_TEST_BIN)
	dd if=/dev/urandom of=$(WG_TEST_IN0) bs=1400 count=20 status=none
	dd if=/dev/urandom of=$(WG_TEST_IN1) bs=5600 count=5 status=none
	dd if=/dev/urandom of=$(WG_TEST_IN2) bs=1400 count=40 status=none
	dd if=/dev/urandom bs=1 count=96 status=none >> $(WG_TEST_IN2)
	./$(WG_BIN) --no-pace --multi \
		$(WG_TEST_IN0) $(WG_TEST_OUT0) \
		$(WG_TEST_IN1) $(WG_TEST_OUT1) \
		$(WG_TEST_IN2) $(WG_TEST_OUT2)
	cmp $(WG_TEST_IN0) $(WG_TEST_OUT0)
	cmp $(WG_TEST_IN1) $(WG_TEST_OUT1)
	cmp $(WG_TEST_IN2) $(WG_TEST_OUT2)
	sh $(TEST_DIR)/wire_loopback_test.sh ./$(WG_BIN) $(OBJ_DIR)
	sh $(TEST_DIR)/wire_multi_flow_test.sh ./$(WG_BIN) $(OBJ_DIR)
	sh $(TEST_DIR)/wire_xor_fec_test.sh ./$(WG_BIN) $(OBJ_DIR)
	sh $(TEST_DIR)/wire_rs_fec_test.sh ./$(WG_BIN) $(OBJ_DIR)
	sh $(TEST_DIR)/wire_rs_test.sh ./$(WG_BIN) $(OBJ_DIR)
	sh $(TEST_DIR)/wire_wirehair_test.sh ./$(WG_BIN) $(OBJ_DIR)
	sh $(TEST_DIR)/wire_relay_loopback.sh ./$(WG_BIN) ./$(RELAY_BIN) $(OBJ_DIR)

sanitize: CFLAGS += -fsanitize=address,undefined -fno-omit-frame-pointer
sanitize: LDFLAGS += -fsanitize=address,undefined
sanitize: clean test integration-test

tsan: CFLAGS += -fsanitize=thread -fno-omit-frame-pointer
tsan: LDFLAGS += -fsanitize=thread
tsan: clean test integration-test

$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

INCLUDE_HDRS := $(wildcard include/*.h)
WG_HDRS := $(wildcard $(WG_DIR)/*.h)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c $(INCLUDE_HDRS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(OBJ_DIR)/circular_buffer.o: $(CB_SRC) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(LIB): $(LIB_OBJS) | $(OBJ_DIR)
	ar rcs $@ $^

$(TEST_BIN): $(TEST_DIR)/run_tests.c $(LIB) | $(OBJ_DIR)
	$(CC) $(CFLAGS) $(TEST_DIR)/run_tests.c $(LIB) -o $@ $(LDFLAGS)

$(OBJ_DIR)/wg_%.o: $(WG_DIR)/%.c $(INCLUDE_HDRS) $(WG_HDRS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(WG_DIR) -c $< -o $@

$(WIREHAIR_LIB): $(WIREHAIR_SRCS) | $(OBJ_DIR)
	$(CMAKE) -S $(WIREHAIR_DIR) -B $(WIREHAIR_BUILD_DIR) \
		-DBUILD_TESTS=OFF -DBUILD_CODEC_V2=OFF -DCMAKE_BUILD_TYPE=Release
	$(CMAKE) --build $(WIREHAIR_BUILD_DIR) --target wirehair

$(OBJ_DIR)/wg_wirehair_segment.o: $(WG_DIR)/wirehair_segment.c \
	$(WG_DIR)/wirehair_segment.h $(WG_DIR)/stream_config.h \
	$(WIREHAIR_DIR)/include/wirehair/wirehair.h | $(OBJ_DIR)
	$(CC) $(CFLAGS) -DWIREHAIR_STATIC -I$(WG_DIR) \
		-I$(WIREHAIR_DIR)/include -c $< -o $@

$(OBJ_DIR)/wg_wirehair_segment_sender.o: $(WG_DIR)/wirehair_segment_sender.c \
	$(WG_DIR)/wirehair_segment_sender.h $(WG_DIR)/wirehair_segment.h \
	$(WG_DIR)/stream_config.h $(INCLUDE_HDRS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -DWIREHAIR_STATIC -I$(WG_DIR) \
		-I$(WIREHAIR_DIR)/include -c $< -o $@

$(OBJ_DIR)/wg_rs_codec.o: $(WG_DIR)/rs_codec.c $(INCLUDE_HDRS) $(WG_HDRS) \
	$(RSCODE_DIR)/ecc.h | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(WG_DIR) -I$(RSCODE_DIR) -c $< -o $@

$(OBJ_DIR)/wg_rs_gf256_avx2.o: $(WG_DIR)/rs_gf256_avx2.c \
	$(WG_DIR)/rs_gf256_simd.h | $(OBJ_DIR)
	$(CC) $(CFLAGS) $(RS_AVX2_CFLAGS) -I$(WG_DIR) -c $< -o $@

$(OBJ_DIR)/wg_rs_gf256_ssse3.o: $(WG_DIR)/rs_gf256_ssse3.c \
	$(WG_DIR)/rs_gf256_simd.h | $(OBJ_DIR)
	$(CC) $(CFLAGS) $(RS_SSSE3_CFLAGS) -I$(WG_DIR) -c $< -o $@

$(OBJ_DIR)/rscode_%.o: $(RSCODE_DIR)/%.c $(RSCODE_DIR)/ecc.h | $(OBJ_DIR)
	$(CC) $(CFLAGS) -Wno-unused-parameter -I$(RSCODE_DIR) -c $< -o $@

$(WG_BIN): $(WG_OBJS) $(RSCODE_OBJS) $(LIB_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(WG_DIR) $(WG_OBJS) $(RSCODE_OBJS) $(LIB_OBJS) -o $@ $(LDFLAGS) $(RS_LDFLAGS)

$(OBJ_DIR)/relay_%.o: $(RELAY_DIR)/%.c $(INCLUDE_HDRS) $(RELAY_HDRS) $(WG_HDRS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(RELAY_DIR) -I$(WG_DIR) -c $< -o $@

$(OBJ_DIR)/relay_deferred.o: $(RELAY_DIR)/relay_deferred.c $(INCLUDE_HDRS) $(RELAY_HDRS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(RELAY_DIR) -c $< -o $@

$(RELAY_BIN): $(RELAY_OBJS) $(OBJ_DIR)/wire_header.o $(RELAY_DECODE_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(RELAY_DIR) -I$(WG_DIR) $(RELAY_OBJS) $(OBJ_DIR)/wire_header.o \
		$(RELAY_DECODE_OBJS) -o $@ $(LDFLAGS) $(RS_LDFLAGS)

$(RELAY_GEN_CACHE_TEST_BIN): $(TEST_DIR)/relay_gen_cache_tests.c $(RELAY_CORE_OBJS) \
	$(OBJ_DIR)/relay_local_source.o $(OBJ_DIR)/wire_header.o $(RELAY_DECODE_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(RELAY_DIR) -I$(WG_DIR) $(TEST_DIR)/relay_gen_cache_tests.c \
		$(RELAY_CORE_OBJS) $(OBJ_DIR)/relay_local_source.o $(OBJ_DIR)/wire_header.o \
		$(RELAY_DECODE_OBJS) -o $@ $(LDFLAGS) $(RS_LDFLAGS)

$(RELAY_EGRESS_QUEUE_TEST_BIN): $(TEST_DIR)/relay_egress_queue_tests.c \
	$(OBJ_DIR)/relay_egress_queue.o | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(RELAY_DIR) $(TEST_DIR)/relay_egress_queue_tests.c \
		$(OBJ_DIR)/relay_egress_queue.o -o $@ $(LDFLAGS)

$(RELAY_DEFERRED_TEST_BIN): $(TEST_DIR)/relay_deferred_tests.c $(RELAY_CORE_OBJS) \
	$(OBJ_DIR)/relay_local_source.o $(OBJ_DIR)/wire_header.o $(RELAY_DECODE_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(RELAY_DIR) -I$(WG_DIR) $(TEST_DIR)/relay_deferred_tests.c \
		$(RELAY_CORE_OBJS) $(OBJ_DIR)/relay_local_source.o $(OBJ_DIR)/wire_header.o \
		$(RELAY_DECODE_OBJS) -o $@ $(LDFLAGS) $(RS_LDFLAGS)

$(RELAY_LOCAL_DECODE_TEST_BIN): $(TEST_DIR)/relay_local_decode_tests.c \
	$(RELAY_LIB_OBJS) $(OBJ_DIR)/wire_header.o $(RELAY_DECODE_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(RELAY_DIR) -I$(WG_DIR) \
		$(TEST_DIR)/relay_local_decode_tests.c \
		$(RELAY_LIB_OBJS) $(OBJ_DIR)/wire_header.o $(RELAY_DECODE_OBJS) \
		-o $@ $(LDFLAGS) $(RS_LDFLAGS)

$(RELAY_LOCAL_SOURCE_TEST_BIN): $(TEST_DIR)/relay_local_source_tests.c \
	$(RELAY_LIB_OBJS) $(OBJ_DIR)/wire_header.o $(RELAY_DECODE_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(RELAY_DIR) -I$(WG_DIR) \
		$(TEST_DIR)/relay_local_source_tests.c \
		$(RELAY_LIB_OBJS) $(OBJ_DIR)/wire_header.o $(RELAY_DECODE_OBJS) \
		-o $@ $(LDFLAGS) $(RS_LDFLAGS)

$(RS_BATS_RECODER_TEST_BIN): $(TEST_DIR)/rs_bats_recoder_tests.c \
	$(OBJ_DIR)/relay_rs_bats_recoder.o $(OBJ_DIR)/relay_generation_cache.o \
	$(OBJ_DIR)/wire_header.o | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(RELAY_DIR) -I$(WG_DIR) \
		$(TEST_DIR)/rs_bats_recoder_tests.c \
		$(OBJ_DIR)/relay_rs_bats_recoder.o \
		$(OBJ_DIR)/relay_generation_cache.o \
		$(OBJ_DIR)/wire_header.o \
		-o $@ $(LDFLAGS)

$(WIRE_UDP_RECV_DEMUX_TEST_BIN): $(TEST_DIR)/wire_udp_recv_demux_tests.c \
	$(OBJ_DIR)/wg_wire_udp.o $(OBJ_DIR)/wg_wire_flow_decoder.o \
	$(OBJ_DIR)/wg_codec.o $(OBJ_DIR)/wg_block_codec.o $(OBJ_DIR)/wg_copy_codec.o \
	$(OBJ_DIR)/wg_xor_fec_codec.o $(OBJ_DIR)/wg_rs_fec_codec.o \
	$(OBJ_DIR)/wg_rs_codec.o $(RSCODE_OBJS) $(OBJ_DIR)/wire_header.o | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(WG_DIR) \
		$(TEST_DIR)/wire_udp_recv_demux_tests.c \
		$(OBJ_DIR)/wg_wire_udp.o $(OBJ_DIR)/wg_wire_flow_decoder.o \
		$(OBJ_DIR)/wg_codec.o $(OBJ_DIR)/wg_block_codec.o $(OBJ_DIR)/wg_copy_codec.o \
		$(OBJ_DIR)/wg_xor_fec_codec.o $(OBJ_DIR)/wg_rs_fec_codec.o \
		$(OBJ_DIR)/wg_rs_codec.o $(RSCODE_OBJS) $(OBJ_DIR)/wire_header.o \
		-o $@ $(LDFLAGS) $(RS_LDFLAGS)

$(WIRE_FLOW_DECODER_RS_TEST_BIN): $(TEST_DIR)/wire_flow_decoder_rs_tests.c \
	$(OBJ_DIR)/wg_wire_flow_decoder.o $(OBJ_DIR)/wg_codec.o \
	$(OBJ_DIR)/wg_block_codec.o $(OBJ_DIR)/wg_copy_codec.o \
	$(OBJ_DIR)/wg_xor_fec_codec.o $(OBJ_DIR)/wg_rs_fec_codec.o \
	$(OBJ_DIR)/wg_rs_codec.o $(RSCODE_OBJS) $(OBJ_DIR)/wire_header.o | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(WG_DIR) \
		$(TEST_DIR)/wire_flow_decoder_rs_tests.c \
		$(OBJ_DIR)/wg_wire_flow_decoder.o $(OBJ_DIR)/wg_codec.o \
		$(OBJ_DIR)/wg_block_codec.o $(OBJ_DIR)/wg_copy_codec.o \
		$(OBJ_DIR)/wg_xor_fec_codec.o $(OBJ_DIR)/wg_rs_fec_codec.o \
		$(OBJ_DIR)/wg_rs_codec.o $(RSCODE_OBJS) $(OBJ_DIR)/wire_header.o \
		-o $@ $(LDFLAGS) $(RS_LDFLAGS)

$(WIRE_FLOW_DECODER_SYS_TEST_BIN): $(TEST_DIR)/wire_flow_decoder_systematic_tests.c \
	$(OBJ_DIR)/wg_wire_flow_decoder.o $(OBJ_DIR)/wg_codec.o \
	$(OBJ_DIR)/wg_block_codec.o $(OBJ_DIR)/wg_copy_codec.o \
	$(OBJ_DIR)/wg_xor_fec_codec.o $(OBJ_DIR)/wg_rs_fec_codec.o \
	$(OBJ_DIR)/wg_rs_codec.o $(RSCODE_OBJS) $(OBJ_DIR)/wire_header.o | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(WG_DIR) \
		$(TEST_DIR)/wire_flow_decoder_systematic_tests.c \
		$(OBJ_DIR)/wg_wire_flow_decoder.o $(OBJ_DIR)/wg_codec.o \
		$(OBJ_DIR)/wg_block_codec.o $(OBJ_DIR)/wg_copy_codec.o \
		$(OBJ_DIR)/wg_xor_fec_codec.o $(OBJ_DIR)/wg_rs_fec_codec.o \
		$(OBJ_DIR)/wg_rs_codec.o $(RSCODE_OBJS) $(OBJ_DIR)/wire_header.o \
		-o $@ $(LDFLAGS) $(RS_LDFLAGS)

$(WIRE_FLOW_DECODER_REORDER_TEST_BIN): $(TEST_DIR)/wire_flow_decoder_reorder_tests.c \
	$(OBJ_DIR)/wg_wire_flow_decoder.o $(OBJ_DIR)/wg_codec.o \
	$(OBJ_DIR)/wg_block_codec.o $(OBJ_DIR)/wg_copy_codec.o \
	$(OBJ_DIR)/wg_xor_fec_codec.o $(OBJ_DIR)/wg_rs_fec_codec.o \
	$(OBJ_DIR)/wg_rs_codec.o $(RSCODE_OBJS) $(OBJ_DIR)/wire_header.o | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(WG_DIR) \
		$(TEST_DIR)/wire_flow_decoder_reorder_tests.c \
		$(OBJ_DIR)/wg_wire_flow_decoder.o $(OBJ_DIR)/wg_codec.o \
		$(OBJ_DIR)/wg_block_codec.o $(OBJ_DIR)/wg_copy_codec.o \
		$(OBJ_DIR)/wg_xor_fec_codec.o $(OBJ_DIR)/wg_rs_fec_codec.o \
		$(OBJ_DIR)/wg_rs_codec.o $(RSCODE_OBJS) $(OBJ_DIR)/wire_header.o \
		-o $@ $(LDFLAGS) $(RS_LDFLAGS)

$(WG_CODEC_TEST_BIN): $(TEST_DIR)/wg_codec_tests.c \
	$(OBJ_DIR)/wg_codec.o $(OBJ_DIR)/wg_block_codec.o $(OBJ_DIR)/wg_copy_codec.o \
	$(OBJ_DIR)/wg_xor_fec_codec.o $(OBJ_DIR)/wg_rs_fec_codec.o \
	$(OBJ_DIR)/wg_rs_codec.o $(RSCODE_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(WG_DIR) $^ -o $@ $(LDFLAGS) $(RS_LDFLAGS)

$(RS_RECOVERY_BENCH_BIN): $(TEST_DIR)/rs_recovery_bench.c \
	$(OBJ_DIR)/wg_codec.o $(OBJ_DIR)/wg_block_codec.o $(OBJ_DIR)/wg_copy_codec.o \
	$(OBJ_DIR)/wg_xor_fec_codec.o $(OBJ_DIR)/wg_rs_fec_codec.o \
	$(OBJ_DIR)/wg_rs_codec.o $(RSCODE_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(WG_DIR) $^ -o $@ $(LDFLAGS) $(RS_LDFLAGS)

$(RS_ENCODE_BENCH_BIN): $(TEST_DIR)/rs_encode_bench.c \
	$(OBJ_DIR)/wg_codec.o $(OBJ_DIR)/wg_block_codec.o $(OBJ_DIR)/wg_copy_codec.o \
	$(OBJ_DIR)/wg_xor_fec_codec.o $(OBJ_DIR)/wg_rs_fec_codec.o \
	$(OBJ_DIR)/wg_rs_codec.o $(RSCODE_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(WG_DIR) $^ -o $@ $(LDFLAGS) $(RS_LDFLAGS)

$(RS_ENCODE_FAST_TEST_BIN): $(TEST_DIR)/rs_encode_fast_tests.c \
	$(OBJ_DIR)/wg_codec.o $(OBJ_DIR)/wg_block_codec.o $(OBJ_DIR)/wg_copy_codec.o \
	$(OBJ_DIR)/wg_xor_fec_codec.o $(OBJ_DIR)/wg_rs_fec_codec.o \
	$(OBJ_DIR)/wg_rs_codec.o $(RSCODE_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(WG_DIR) $^ -o $@ $(LDFLAGS) $(RS_LDFLAGS)

$(RS_ENCODE_GENERAL_TEST_BIN): $(TEST_DIR)/rs_encode_general_tests.c \
	$(OBJ_DIR)/wg_codec.o $(OBJ_DIR)/wg_block_codec.o $(OBJ_DIR)/wg_copy_codec.o \
	$(OBJ_DIR)/wg_xor_fec_codec.o $(OBJ_DIR)/wg_rs_fec_codec.o \
	$(OBJ_DIR)/wg_rs_codec.o $(RSCODE_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(WG_DIR) $^ -o $@ $(LDFLAGS) $(RS_LDFLAGS)

$(OBJ_DIR)/fec_transport.o: $(SRC_DIR)/fec_transport.c include/fec_transport.h \
	$(INCLUDE_HDRS) $(WG_HDRS) $(WG_DIR)/wirehair_segment.h \
	$(WG_DIR)/wirehair_segment_sender.h | $(OBJ_DIR)
	$(CC) $(CFLAGS) -DWIREHAIR_STATIC -I$(WG_DIR) -I$(RSCODE_DIR) \
		-I$(WIREHAIR_DIR)/include -c $< -o $@

$(FEC_TRANSPORT_TEST_BIN): $(TEST_DIR)/fec_transport_tests.c \
	$(OBJ_DIR)/fec_transport.o $(OBJ_DIR)/wire_header.o \
	$(OBJ_DIR)/wg_codec.o $(OBJ_DIR)/wg_block_codec.o $(OBJ_DIR)/wg_copy_codec.o \
	$(OBJ_DIR)/wg_xor_fec_codec.o $(OBJ_DIR)/wg_rs_fec_codec.o \
	$(OBJ_DIR)/wg_rs_codec.o $(RSCODE_OBJS) \
	$(OBJ_DIR)/wg_wirehair_segment.o $(OBJ_DIR)/wg_wirehair_segment_sender.o \
	$(WIREHAIR_LIB) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(WG_DIR) -I$(WIREHAIR_DIR)/include $^ -o $@ \
		$(LDFLAGS) $(RS_LDFLAGS)

$(FEC_INTERLEAVE_SIM_BIN): $(TEST_DIR)/fec_interleave_sim.c \
	$(OBJ_DIR)/wg_codec.o $(OBJ_DIR)/wg_block_codec.o $(OBJ_DIR)/wg_copy_codec.o \
	$(OBJ_DIR)/wg_xor_fec_codec.o $(OBJ_DIR)/wg_rs_fec_codec.o \
	$(OBJ_DIR)/wg_rs_codec.o $(RSCODE_OBJS) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(WG_DIR) $^ -o $@ $(LDFLAGS) $(RS_LDFLAGS)

$(FEC_TRACE_BIN): $(TEST_DIR)/fec_trace.c \
	$(OBJ_DIR)/wg_codec.o $(OBJ_DIR)/wg_block_codec.o \
	$(OBJ_DIR)/wg_copy_codec.o $(OBJ_DIR)/wg_xor_fec_codec.o \
	$(OBJ_DIR)/wg_rs_fec_codec.o | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(WG_DIR) $^ -o $@ $(LDFLAGS) $(RS_LDFLAGS)

$(WIREHAIR_SEGMENT_TEST_BIN): $(TEST_DIR)/wirehair_segment_tests.c \
	$(OBJ_DIR)/wg_wirehair_segment.o $(OBJ_DIR)/wire_header.o \
	$(WIREHAIR_LIB) | $(OBJ_DIR)
	$(CC) $(CFLAGS) -I$(WG_DIR) -I$(WIREHAIR_DIR)/include $^ -o $@ \
		$(LDFLAGS) -lstdc++ -lm

clean:
	rm -rf $(OBJ_DIR)
