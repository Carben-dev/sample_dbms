# Feb. 23rd 2019
import os

if __name__ == "__main__":
    external_sort_binary = "./external_sorting_binary"
    inDB = "names.db"
    pSizes = ["512","1024","2048"]
    Bs = ["3", "10", "20", "50", "100", "200", "500","1000", "5000", "10000"]
    fields = ["1"]

    for B in Bs:
        for pSize in pSizes:
            for field in fields:
                print("-----------------------------")
                print("pSize = " + pSize + ". " + "B = " + B + ". " + "field = " + field + ".")
                outDB = "sorted_" + B + "_" + pSize + ".db"
                os.system(external_sort_binary+" "+inDB+" "+outDB+" "+B+" "+pSize+" "+field)
