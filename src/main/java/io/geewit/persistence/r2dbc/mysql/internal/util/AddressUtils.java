/*
 * Copyright 2023 geewit.io projects
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.geewit.persistence.r2dbc.mysql.internal.util;

/**
 * A utility for matching host/address.
 */
public final class AddressUtils {

    /**
     * Checks if the host is an address of IP version 4.
     *
     * @param host the host to check.
     * @return true if it is a valid IPv4 address, false otherwise.
     */
    public static boolean isIpv4(String host) {
        if (host == null || host.isEmpty()) {
            return false;
        }

        int segments = 0;
        int value = 0;
        boolean digitFound = false;

        // 遍历每个字符，使用 Java 23 增强的 switch 表达式
        for (char c : host.toCharArray()) {
            if (c == '.') { // 遇到点之前必须有数字，否则为空段
                if (!digitFound) {
                    return false;
                }
                segments++;
                // IPv4 应该只有三个点（四个段）
                if (segments > 3) {
                    return false;
                }
                // 重置下一个段的值
                value = 0;
                digitFound = false;
            } else {
                if (c >= '0' && c <= '9') {
                    value = value * 10 + (c - '0');
                    // 超过 255 立即返回 false
                    if (value > 255) {
                        return false;
                    }
                    digitFound = true;
                } else {
                    return false;
                }
            }
        }
        // 循环结束后，必须有最后一个段，并且总共应有 3 个点分隔符
        return digitFound && segments == 3;
    }

    /**
     * Checks if the host is an address of IP version 6.
     *
     * @param host the host to check.
     * @return true if it is a valid IPv6 address, false otherwise.
     */
    public static boolean isIpv6(String host) {
        if (host == null || host.isEmpty()) {
            return false;
        }
        // 如果包含 "::" 则可能是压缩格式
        if (host.contains("::")) {
            return isIpv6Compressed(host);
        } else {
            // 非压缩格式必须分成 8 组（7 个冒号）
            String[] groups = host.split(":", -1);
            if (groups.length != 8) {
                return false;
            }
            for (String group : groups) {
                if (isInvalidIpv6Group(group)) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Checks if the host is an address of compressed IPv6.
     *
     * @param host the host to check.
     * @return true if it is a valid compressed IPv6 address, false otherwise.
     */
    private static boolean isIpv6Compressed(String host) {
        // 提前计数冒号数，若超过 7 个则不可能是有效地址
        int colonCount = 0;
        for (char c : host.toCharArray()) {
            if (c == ':') {
                colonCount++;
            }
        }
        if (colonCount > 7) {
            return false;
        }

        // "::" 只能出现一次
        int firstIndex = host.indexOf("::");
        int lastIndex = host.lastIndexOf("::");
        if (firstIndex != lastIndex) {
            return false;
        }

        // 按 "::" 分割地址，左右两侧分别验证
        String[] parts = host.split("::", -1);
        if (parts.length > 2) {
            return false;
        }

        int groupsCount = 0;
        // 处理左侧部分
        if (!parts[0].isEmpty()) {
            String[] leftGroups = parts[0].split(":", -1);
            for (String group : leftGroups) {
                if (isInvalidIpv6Group(group)) {
                    return false;
                }
            }
            groupsCount += leftGroups.length;
        }
        // 处理右侧部分（可能不存在）
        if (parts.length == 2 && !parts[1].isEmpty()) {
            String[] rightGroups = parts[1].split(":", -1);
            for (String group : rightGroups) {
                if (isInvalidIpv6Group(group)) {
                    return false;
                }
            }
            groupsCount += rightGroups.length;
        }
        // 压缩格式中，组数必须少于 8，因为 "::" 至少替代一组全 0
        return groupsCount < 8;
    }

    /**
     * Validates an individual group of an IPv6 address.
     * 注意：该方法已反转逻辑，即返回 true 表示该组无效，
     * 从而在调用处无需使用逻辑非运算符来反转结果。
     *
     * @param group the group to validate.
     * @return true if the group is not a valid hexadecimal number with 1 to 4 digits, false otherwise.
     */
    private static boolean isInvalidIpv6Group(String group) {
        if (group == null || group.isEmpty() || group.length() > 4) {
            return true;
        }
        for (int i = 0; i < group.length(); i++) {
            char c = group.charAt(i);
            // 使用 Java 23 增强的 switch 表达式判断是否为合法的十六进制字符
            boolean isNotHex = switch (c) {
                case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                     'a', 'b', 'c', 'd', 'e', 'f',
                     'A', 'B', 'C', 'D', 'E', 'F' -> false;
                default -> true;
            };
            if (isNotHex) {
                return true;
            }
        }
        return false;
    }

    private AddressUtils() { }
}
