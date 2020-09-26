#!/usr/bin/python3.6
#67.195.114.50 - - [20/May/2010:07:35:27 +0100] "GET /post/261556/ HTTP/1.0" 404 15 "-" "Mozilla/5.0 (compatible; Yahoo! Slurp/3.0; http://help.yahoo.com/help/us/ysearch/slurp)"
import random
import datetime
import getopt
import sys

def gen_ip_addr():
    return "%s.%s.%s.%s" % (random.randint(83,83), random.randint(167,167), random.randint(100,122), random.randint(0,255))

def gen_url():
    return "https://mysite.com"

def gen_datetime():
    dt = datetime.datetime(2020, random.randint(1,9), random.randint(1,22), random.randint(1,23), random.randint(1,59), random.randint(1,59))
    return dt.strftime("%d/%b/%Y:%H:%M:%S")

def gen_bytes_count():
    return random.randint(50, 10200)

def gen_user_agent(agents):
    return random.choice(agents)

def gen_http_response():
    codes = [100,101,102,200,201,202,203,204,205,
             206,207,208,226,301,302,303,304,305,
             306,307,308,400,401,402,403,404,405,
             406,407,408,409,410,411,412,413,414,
             415,416,417,418,420,422,423,424,425,
             426,428,429,431,444,449,450,451,499,
             500,501,502,503,504,505,506,507,508,
             509,510,511,598,599]
    return random.choice(codes)

def gen_line(agents):
    return "%s - - [%s +0100] \"GET %s\" %s %s \"-\"%s" %(
        gen_ip_addr(),
        gen_datetime(),
        gen_url(),
        gen_http_response(),
        gen_bytes_count(),
        gen_user_agent(agents)
    )

def gen_error(line):
    isErr = random.choices([0,1], weights=[90,10], k=1)
    if not isErr[0]:
        return line
    else:
        result = ""
        arr = line.split(" ")
        first = True
        for i in arr:
            if not first:
                is_space = random.choices([0,1], weights=[50,50], k=1)
                if is_space[0]:
                    result += " "

            first = False
            err = random.choices([0,1], weights=[30,70], k=1)
            if err[0]:
                lst = list(i)
                random.shuffle(lst)
                i = ''.join(lst)
                i = i[0:random.randint(0, int(len(i)/2))]
                #i = "*" * int(len(i)-random.randint(0, int(len(i)/2)))
            result += i;
        return result

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "ho:s:c:", ["help", "output=", "size=", "count="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(str(err))  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    output = None
    size = 0
    splits_count = 0
    for o, a in opts:
        if o in ("-s", "--size"):
            size = int(a)
        elif o in ("-h", "--help"):
            sys.exit()
        elif o in ("-o", "--output"):
            output = a
        elif o in ("-c", "--count"):
            splits_count = int(a)
        else:
            print("unhandled option")

    agents = []

    with open("user_agents.txt", 'r') as fp:
        agents = fp.read().splitlines()

    limit_size = size*1024*1024
    for i in range(0, splits_count):
        sum_size = 0
        f = open(output+"_"+str(i)+".log", "w")
        while sum_size < limit_size:
            line = gen_line(agents)
            line = gen_error(line)
            sum_size += len(line)+1
            f.write(line+'\n')
        f.close()
        print("Generated %s/%s" % (i+1, splits_count))

if __name__ == "__main__":
    print("Started!")
    main()
    print("Done!")
