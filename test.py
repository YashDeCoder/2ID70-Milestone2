varString = "R,a;b;c,1;2;3"

def wc_flatmap(r):
    l = []
    words = r.split(",")
    for word in words:
        l.append(word)
    return l

l = wc_flatmap(varString)

def wc_mappingrdd(l):
    m = []
    
    records = l[1].split(";") 
    numbers = l[2].split(";")
    # for char in records:
    #     m.append(l[0] + "," + char)
    # for num in range(0, len(numbers)):
    #     m[num] = m[num] + numbers[num]

    for (char, num) in zip(records, numbers):
        m.append(l[0] + "," + char + "," + num)
    return m




