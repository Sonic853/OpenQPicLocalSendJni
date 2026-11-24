LOCAL_PATH := $(call my-dir)
include $(CLEAR_VARS)
LOCAL_MODULE := qpiclocalsend
LOCAL_SRC_FILES := ../src/transport.c ../third_party/picohttpparser/picohttpparser.c
LOCAL_C_INCLUDES := $(LOCAL_PATH)/../third_party/picohttpparser
LOCAL_CFLAGS := -std=c99 -Os -Wall -Wextra -Wno-unused-parameter -fvisibility=hidden
LOCAL_LDFLAGS := -Wl,--version-script=$(LOCAL_PATH)/exports.map
LOCAL_ADDITIONAL_DEPENDENCIES := $(LOCAL_PATH)/exports.map
include $(BUILD_SHARED_LIBRARY)
