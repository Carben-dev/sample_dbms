import sys
import hashlib as hl
import math
import matplotlib.pyplot as plot

# debug flag
debug = 1

# Global Var
nextPageNum = 1  # next available page, starting from 1, 0 reserved for Super Page
rowid = 0  # rowid counter
indexType = 9999
global inDB_file
global indexFile_file

# Global Counter
buckets_count = 0
reg_pages_count = 0
overflow_pages_count = 0

plot_list = []  # for plotting

########################################################################################
################################## Util Function  ######################################
########################################################################################


def getNextPageNum():
    global nextPageNum
    prev = nextPageNum
    nextPageNum += 1
    return prev


def getRecord():
    global rowid
    record = inDB_file.read(64)
    ret_rid = rowid
    rowid += 1
    if 1:
        if rowid % 1000 == 0:
            print("record:"+str(rowid))
    return record, ret_rid


def md5(key):
    return int(hl.md5(key.encode()).hexdigest(),16)

########################################################################################
##################################   Util Class   ######################################
########################################################################################


class Page:
    def __init__(self, pNum, pSize, pCapacity, pType, keySize, localDepth=0):
        self.indexs = []
        self.pageNum = pNum
        self.pageSize = pSize
        self.capacity = pCapacity

        # info need to writen to file
        self.type = pType
        self.overflow = None
        self.indexsCount = 0
        self.keySize = keySize

        # for Extendible only
        self.localDepth = localDepth

    def addIndex(self, key, rid):
        # Check if there is a overflow page
        if self.overflow is None:
            # check if this page is full
            if self.isFull():
                # create a new page and add it to overflow
                newPage = Page(getNextPageNum(), self.pageSize, self.capacity, 2, self.keySize)
                newPage.addIndex(key, rid)
                self.overflow = newPage
            else:
                # just add it
                self.indexs.append([key, rid])
                self.indexsCount += 1
        else:
            self.overflow.addIndex(key, rid)

    def isFull(self):
        return self.indexsCount == self.capacity

    def writeToFile(self, target_fd):
        # Write this bucket to file according to it's page number
        byte_written = 0
        target_fd.seek(self.pageNum * self.pageSize, 0)

        # write page type
        byte_written += target_fd.write(self.type.to_bytes(4, "big"))

        # write additional info (overflow page num)
        if self.overflow is not None:
            overflow_page_num = self.overflow.pageNum
        else:
            overflow_page_num = 0

        byte_written += target_fd.write(overflow_page_num.to_bytes(4, "big"))

        # write index count
        byte_written += target_fd.write(self.indexsCount.to_bytes(4, "big"))

        # write key size
        key_size = 12
        byte_written += target_fd.write(key_size.to_bytes(4, "big"))

        # write index
        for index in self.indexs:
            key, rid = index
            byte_written += target_fd.write(key.encode("ascii"))
            byte_written += target_fd.write(rid.to_bytes(4, "big"))

        # padding
        padding = self.pageSize - byte_written
        zero = 0
        if padding == 0:
            return
        else:
            target_fd.write(zero.to_bytes(padding, "big"))

########################################################################################
################################## Static Hashing ######################################
########################################################################################

class StaticBucket:
    def __init__(self, pNum, pCapacity, pSize, pType):
        self.indexs = []
        self.overflow = None
        self.pageNum = pNum
        self.indexsCount = 0
        self.pageCapacity = pCapacity
        self.pageSize = pSize
        self.pageType = pType

    def addIndex(self, index):
        # find right place to add the record
        if self.isFull():
            if self.overflow == None:
                self.overflow = StaticBucket(getNextPageNum(), self.pageCapacity, self.pageSize, 2)
                self.overflow.addIndex(index)
            else:
                self.overflow.addIndex(index)
        else:
            self.indexs.append(index)
            self.indexsCount += 1

    def isFull(self):
        return self.indexsCount == self.pageCapacity

    def writeToFile(self, file):

        # Write this bucket to file according to it's page number
        # To Do ...
        byte_written = 0
        file.seek(self.pageNum * self.pageSize, 0)

        # write page type
        byte_written += file.write(self.pageType.to_bytes(4,"big"))

        # write additional info (overflow page num)
        if self.overflow != None:
            overflow_page_num = self.overflow.pageNum
        else:
            overflow_page_num = 0
        byte_written += file.write(overflow_page_num.to_bytes(4,"big"))

        # write index count
        byte_written += file.write(self.indexsCount.to_bytes(4,"big"))

        # write key size
        key_size = 12
        byte_written += file.write(key_size.to_bytes(4,"big"))

        # write index
        for index in self.indexs:
            key, rid = index
            byte_written += file.write(key.encode("ascii"))
            byte_written += file.write(rid.to_bytes(4,"big"))

        # padding
        padding = self.pageSize - byte_written
        zero = 0
        if padding == 0:
            return
        else:
            file.write(zero.to_bytes(padding,"big"))

def buildStaticHashingIndex(pSize, total_records_count, bucket_count, field, keySize):
    global reg_pages_count
    global overflow_pages_count
    global plot_list
    # compute page Capacity
    index_per_page = (pSize - 16)//(keySize + 4)

    # build primary bucket
    primaryBuckets = []
    # first page has less space
    for i in range(bucket_count):
        primaryBuckets.append(StaticBucket(getNextPageNum(), index_per_page, pSize, 1))

    for i in range(total_records_count):
        record, rid = getRecord()
        key = record[:keySize]
        hash = int(hl.md5(key.encode()).hexdigest(),16)
        bucketIdx = hash%(bucket_count)
        primaryBuckets[bucketIdx].addIndex([record[:keySize], rid])

    # we can do plot here
    for bucket in primaryBuckets:
        counter = 1
        curr = bucket
        while curr.overflow is not None:
            curr = curr.overflow
            counter+=1
        plot_list.append(counter)

    # here all records are properly hash into it's bucket
    # next step
    # go from bucket to bucket, write each bucket to file

    # write file header first
    # write page size
    indexFile_file.write(pSize.to_bytes(4,"big"))
    # write index type
    indexFile_file.write(indexType.to_bytes(4,"big"))
    # write primary buckets count
    indexFile_file.write(len(primaryBuckets).to_bytes(4, "big"))

    # write reserved space with 0
    # zero = 0
    # indexFile_file.write(zero.to_bytes(8,"big"))
    # start write page
    for pBucket in primaryBuckets:
        curr = pBucket
        curr.writeToFile(indexFile_file)
        reg_pages_count += 1
        while curr.overflow != None:
            curr = curr.overflow
            curr.writeToFile(indexFile_file)
            overflow_pages_count += 1

    # print result
    # for static hashing bucket count = init bucket
    print("-----------------------")
    print("Static hashing report: (pSize = %d on First Name)"%pageSize)
    print("buckets count: " + str(bucket_count))
    print("reg page count: " + str(reg_pages_count))
    print("overflow page count: " + str(overflow_pages_count))

    plot.hist(plot_list, bins=10)
    plot.xlabel('Number of Pages')
    plot.ylabel('Number of Buckets')
    plot.show()
    return

########################################################################################
############################## Extendible Hashing ######################################
########################################################################################

class Directory:
    def __init__(self, pSize, intiBucketsNum):
        self.pageType = 3
        self.globalDepth = int(round(math.log2(initBucketsNum)))
        self.buckets = []
        self.entries = []
        # according initBuckNum build entries
        for i in range(intiBucketsNum):
            self.buckets.append(ExtendibleBucket(self, 1, getNextPageNum(), pSize, 12, self.globalDepth))
        for i in range(initBucketsNum):
            self.entries.append(i)


    def getBucketByEntryIdx(self, entryIdx):
        if entryIdx >= len(self.entries):
            print(str(entryIdx) + ", " + str(len(self.entries)))
        if self.entries[entryIdx] >= len(self.buckets):
            print("hello" + str(self.entries[entryIdx]))
        return self.buckets[self.entries[entryIdx]]


    def addIndex(self, key, rid):
        hash = int(hl.md5(key.encode()).hexdigest(), 16)
        hash_bin_string = bin(hash)
        cutoff_idx = len(hash_bin_string) - self.globalDepth
        hash_cut = hash_bin_string[cutoff_idx:]
        entryIdx = int(hash_cut, 2)
        self.getBucketByEntryIdx(entryIdx).addIndex(key, rid, entryIdx)


    def addBucket(self, newBucket, entryIdx):
        # see if local depth == global depth
        # if equal, double entry is needed
        # if <, update all entires with localdepth + 1
        if newBucket.getLocalDepth() > self.globalDepth:
            self.double_entries()
        self.buckets.append(newBucket)
        newBucketIdx = len(self.buckets) - 1

        # need to update all entries
        # format entries correctly

        oldEntryIdxStr = '0' + bin(entryIdx)[2:]
        newEntryIdxStr = '1' + bin(entryIdx)[2:]

        formatted = []
        for i in range(len(self.entries)):
            bin_str = bin(i)[2:]
            if len(bin_str) > newBucket.getLocalDepth():
                bin_str = bin_str[len(bin_str) - newBucket.getLocalDepth():]
            elif len(bin_str) < newBucket.getLocalDepth():
                padding = newBucket.getLocalDepth() - len(bin_str)
                for j in range(padding):
                    bin_str = '0' + bin_str
            formatted.append(bin_str)

        # go over formatted to find the same
        for i, hash_str in enumerate(formatted):
            if hash_str == newEntryIdxStr:
                self.entries[i] = newBucketIdx
            if hash_str == oldEntryIdxStr:
                self.entries[i] = self.entries[entryIdx]


    def double_entries(self):
        for i in range(len(self.entries)):
            self.entries.append(self.entries[i])
        self.globalDepth += 1
        if debug:
            print("Entries doubled!")
            print("Entries size:" + str(len(self.entries)))
            print("GlobalDepth:" + str(self.globalDepth))


    def addentry(self):
        pass


    def getGlobalDepth(self):
        return self.globalDepth


class ExtendibleBucket:
    def __init__(self, dir, pType, pNum, pSize, keySize, initLocalDepth):
        self.indexs = []
        self.dir = dir
        self.pageType = pType
        self.localDepth = initLocalDepth
        self.pageNum = pNum
        self.pageSize = pSize
        self.keySize = keySize
        self.index_per_page = (pSize - 16)//(keySize + 4)
        self.indexsCount = 0
        self.overflow = None

        global buckets_count
        global reg_pages_count
        buckets_count += 1
        reg_pages_count += 1

    def addIndex(self, key, rid, entryIdx):
        global reg_pages_count
        global overflow_pages_count
        # check if this bucket is full
        if self.isFull():
            if self.overflow == None:
                # check if this bucket is splitable
                    # read all indexs in the bucket + the new indexs into a new list

                    # use local depth + 1 to hash it into two buckets, if one of the bucket
                    # is empty, it is not splitable

                # if is not splitable, create overflow page

                # if is splitable, add new bucket to directory
                newlist = []
                for index in self.indexs:
                    newlist.append(index)
                newlist.append([key, rid])

                new0 = []
                new1 = []
                for _key, _rid in newlist:
                    hash = int(hl.md5(_key.encode()).hexdigest(), 16)
                    hash_bin_string = bin(hash)
                    cutoff_idx = len(hash_bin_string) - (self.localDepth+1)
                    hash_cut_bit = hash_bin_string[cutoff_idx:cutoff_idx+1]
                    if hash_cut_bit == '0':
                        new0.append([_key, _rid])
                    elif hash_cut_bit == '1':
                        new1.append([_key, _rid])
                    else:
                        print("fatal error, exit")
                        exit(-1)

                if len(new1) == 0 or len(new0) == 0: # Not splitable
                    overflow = ExtendibleBucket(self.dir, 2, getNextPageNum(), self.pageSize, self.keySize,
                                                self.localDepth)
                    overflow.addIndex(key, rid, entryIdx)
                    self.overflow = overflow
                    overflow_pages_count += 1
                else: # split the bucket
                    self.indexs.clear()
                    self.indexsCount = 0
                    self.localDepth += 1
                    newBucket = ExtendibleBucket(self.dir, 1, getNextPageNum(), self.pageSize, self.keySize,
                                                 self.localDepth)
                    for _key, _rid in new0:
                        self.indexs.append([key, rid])
                        self.indexsCount += 1
                    for _key, _rid in new1:
                        newBucket.addIndex(_key, _rid, entryIdx)

                    self.dir.addBucket(newBucket, entryIdx)



            else:
                self.overflow.addIndex(key, rid, entryIdx)

        else:
            # if buck is not full, simply add to bucket
            self.indexs.append([key, rid])
            self.indexsCount += 1

    def changePageType(self, newType):
        self.pageType = newType

    def isEmpty(self):
        return self.indexsCount == 0

    def isFull(self):
        return self.indexsCount == self.index_per_page

    def getLocalDepth(self):
        return self.localDepth


def buildExtendibleHashingIndex(pSize, total_records_count, bucket_count, keySize):
    global reg_pages_count
    global overflow_pages_count

    # create directory
    main_directory = Directory(pSize, bucket_count)
    # for each record in inDB file, add to directory
    for i in range(total_records_count):
        record, rid = getRecord()
        key = record[:keySize]
        main_directory.addIndex(key, rid)

    # here all records are properly hash into it's bucket
    # next step
    # go from bucket to bucket, write each bucket to file

    # write file header first
    # write page size
    indexFile_file.write(pSize.to_bytes(4, "big"))
    # write index type
    indexFile_file.write(indexType.to_bytes(4, "big"))
    # write primary buckets count
    indexFile_file.write(len(main_directory).to_bytes(4, "big"))

    # write reserved space with 0
    # zero = 0
    # indexFile_file.write(zero.to_bytes(8,"big"))
    # start write page
    for dir in main_directory:
        curr = dir
        curr.writeToFile(indexFile_file)
        reg_pages_count += 1
        while curr.overflow is not None:
            curr = curr.overflow
            curr.writeToFile(indexFile_file)
            overflow_pages_count += 1

    # print result
    # for static hashing bucket count = init bucket
    print("-----------------------")
    print("Entendible hashing report: (pSize = %d on First Name)" % pageSize)
    print("buckets count: " + str(bucket_count))
    print("reg page count: " + str(reg_pages_count))
    print("overflow page count: " + str(overflow_pages_count))

    plot.hist(plot_list, bins=10)
    plot.xlabel('Number of Pages')
    plot.ylabel('Number of Buckets')
    plot.show()

    return


########################################################################################
##############################   Linear Hashing   ######################################
########################################################################################

class LinearHashing:
    def __init__(self, initBucketNum, bucketSize):
        self.buckets = []
        self.bucketSize = bucketSize
        self.splitPointer = 0
        self.splitTriggerFlag = False
        self.h_depth = 0
        self.initBucketNum = initBucketNum
        self.currRoundLen = self.initBucketNum

        # Create buckets
        for i in range(self.initBucketNum):
            self.buckets.append(LinearBucket(self.bucketSize))

    def h(self, key):
        return md5(key)%(((2**self.h_depth) * self.initBucketNum))

    def hPlusOne(self, key):
        return md5(key)%(((2**(self.h_depth+1)) * self.initBucketNum))

    def addIndex(self, key, rid):
        # hash the key with h first
        # if hash < splitpoint then rehash with h+1
        hashBucketIdx = self.h(key)
        if hashBucketIdx < self.splitPointer:
            hashBucketIdx = self.hPlusOne(key)

        # append it to the correct bucket
        self.buckets[hashBucketIdx].addIndex(key, rid)

        # update split flag
        self.updateFlag()

        # check if split flas is true, if true do split
        if self.splitTriggerFlag:
            self.split()


    def updateFlag(self):
        for bucket in self.buckets:
            if bucket.isFull():
                self.splitTriggerFlag = True
                return
        self.splitTriggerFlag = False


    def split(self):
        # split the bucket pointer by spliterPointer
        old, new = self.buckets[self.splitPointer].split(self.hPlusOne, self.currRoundLen)
        self.buckets[self.splitPointer] = old
        self.buckets.append(new)

        # move the split pointer to next position
        self.splitPointer += 1

        # check if new split pointer is larger then current round bucket len,
        # if it is larger, we need to set it back to zero and update the hash depth
        # move to next round
        if self.splitPointer >= self.currRoundLen:
            if debug:
                print("Round: %d Complete"%self.h_depth)
                print("Buckets Doubled!")
            self.splitPointer = 0
            self.h_depth += 1
            self.currRoundLen = self.currRoundLen * 2

    def pagefy(self, pSize):
        # pagefy buckets
        result = []
        for i in range(len(self.buckets)):
            result.append(Page(getNextPageNum(), pSize, self.bucketSize, 1, 12))

        for idx, bucket in enumerate(self.buckets):
            for key, rid in bucket.payload:
                result[idx].addIndex(key, rid)

        return result

class LinearBucket:
    def __init__(self, BucketSize):
        self.payload = []
        self.BucketSize = BucketSize

    def isFull(self):
        return len(self.payload) >= self.BucketSize

    def addIndex(self, key, rid):
        self.payload.append([key, rid])

    def split(self, hash_fun, currLength):
        old = LinearBucket(self.BucketSize)
        new = LinearBucket(self.BucketSize)
        for key, rid in self.payload:
            newhash = hash_fun(key)
            if newhash < currLength:
                old.addIndex(key, rid)
            else:
                new.addIndex(key, rid)
        return old, new


def buildLinearHashing(pageSize, total_record_count, initBucketsNum, KeySize):
    global reg_pages_count
    global overflow_pages_count
    global plot_list
    index_per_bucket = round((pageSize-16)/16)
    main_table = LinearHashing(initBucketsNum, index_per_bucket)
    for i in range(total_record_count):
        record, rid = getRecord()
        key = record[:12]
        main_table.addIndex(key, rid)

    # write to file
    # write to file
    # If reach here all records should be properly hash into it's bucket

    # write file header in the first page (page 0)
    # write page size
    indexFile_file.write(pageSize.to_bytes(4, "big"))
    # write index type
    indexFile_file.write(indexType.to_bytes(4, "big"))
    # write the total primary bucket count
    indexFile_file.write(len(main_table.buckets).to_bytes(4, "big"))
    # write split pointer
    indexFile_file.write(main_table.splitPointer.to_bytes(4, "big"))
    # write hash function depth
    indexFile_file.write(main_table.h_depth.to_bytes(4, "big"))
    # write current length
    indexFile_file.write(main_table.currRoundLen.to_bytes(4, "big"))
    # write init bucket number
    indexFile_file.write(main_table.initBucketNum.to_bytes(4, "big"))


    # write reserved space with 0
    # zero = 0

    # start at page 1
    # next step
    # go from bucket to bucket, write each bucket to file
    # indexFile_file.write(zero.to_bytes(8,"big"))
    # start write page
    pages = main_table.pagefy(pageSize)
    for page in pages:
        counter = 1
        curr = page
        curr.writeToFile(indexFile_file)
        reg_pages_count += 1
        while curr.overflow is not None:
            curr = curr.overflow
            curr.writeToFile(indexFile_file)
            overflow_pages_count += 1
            counter += 1
        plot_list.append(counter)
    # print result
    # for static hashing bucket count = init bucket
    print("-----------------------")
    print("Linear hashing report: (pageSize = %d on First Name)" % pageSize)
    print("Numbers of buckets: " + str(len(main_table.buckets)))
    print("Numbers of reg index page: " + str(reg_pages_count))
    print("Numbers of overflow page: " + str(overflow_pages_count))

    plot.hist(plot_list, bins=10)
    plot.xlabel('Number of Pages')
    plot.ylabel('Number of Buckets')
    plot.show()

    return

########################################################################################
##############################         Main       ######################################
########################################################################################

if __name__ == "__main__":
    # Input parameters
    inDB = sys.argv[1]
    indexFile = sys.argv[2]
    indexType = int(sys.argv[3])
    initBucketsNum = int(sys.argv[4])
    pageSize = int(sys.argv[5])
    field = int(sys.argv[6])

    # Open files
    inDB_file = open(inDB,"r")
    indexFile_file = open(indexFile,"wb")

    if indexType == 0:
        # Static hashing
        buildStaticHashingIndex(pageSize, 32000000 // 64, initBucketsNum, 0, 12)
    elif indexType == 1:
        buildExtendibleHashingIndex(pageSize, 32000000 // 64, initBucketsNum, 12)
    elif indexType == 2:
        buildLinearHashing(pageSize, 32000000 // 64, initBucketsNum, 12)
    else:
        print("Wrong indexType.")
        exit(-1)

    inDB_file.close()
    indexFile_file.close()