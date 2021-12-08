# !/usr/bin/python3
# -*- coding: utf-8 -*-
# > Author          : lunar
# > Email           : lunar_ubuntu@qq.com
# > Created Time    : Wed 08 Dec 2021 04:17:10 PM CST
# > Location        : Shanghai
# > Copyright@ https://github.com/xiaoqixian

def rm_win32(path, output_path):
    f = open(path, "r")
    out = open(output_path, "w")

    enter = False
    count = 0
    lin_num = 1

    line = f.readline()
    while len(line) != 0:
        if line.startswith("#ifdef _WIN32"):
            enter = True
            count += 1

        elif enter and line.startswith("#ifdef"):
            count += 1

        elif enter and line.startswith("#if"):
            count += 1

        elif enter and line.startswith("#else"):
            if count == 1:
                enter = False

        elif count > 0 and line.startswith("#endif"):
            if enter and count > 1:
                count -= 1
            elif count == 1:
                count -= 1
                enter = False
            assert count >= 0, "invalid count: %d" % lin_num

        elif not enter:
            out.write(line)

        line = f.readline()
        # print("%d lines" % lin_num)
        lin_num += 1

def add_space(path, output_path):
    f = open(path, 'r')
    out = open(output_path, 'w')

    line = f.readline()
    while len(line) != 0:
        if line.startswith("#ifdef") or line.startswith("#ifndef"):
            out.write("\n" + line)

        elif line.startswith("#endif") or line.startswith("#else"):
            out.write(line + "\n")

        else:
            out.write(line)

        line = f.readline()

def add_comment(path, output_path):
    f = open(path, 'r')
    out = open(output_path, 'w')

    stack = []
    count = 1

    line = f.readline()
    while len(line) != 0:
        if line.startswith("#ifdef") or line.startswith("#ifndef") or line.startswith("#if"):
            out.write(line)
            temp = line.split(" ")
            temp[-1] = temp[-1][:-1]
            stack.append(temp[1:])

        elif line.startswith("#else"):
            out.write(line[:-1] + " /*" + " ".join(stack[-1]) + "*/\n")

        elif line.startswith("#endif"):
            out.write(line[:-1] + " /*" + " ".join(stack[-1]) + "*/\n")
            stack.pop()

        else:
            out.write(line)

        print("%d lines" % count)
        line = f.readline()
        count += 1


if __name__ == "__main__":
    rm_win32("mdb.c", "mdb_cp3.c")
