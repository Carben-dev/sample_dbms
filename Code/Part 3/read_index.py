import sys
import hashlib as hl

global inDB_fd
global indexFile_fd

indexPage_read_count = 0
dataPage_read_count = 0

# hash function
def md5(key):
    return int(hl.md5(key.encode()).hexdigest(),16)

# helper class easy to handle data
class Bucket:
    def __init__(self, pType, overflow, indexsCount, keySize, localDepth=0):
        self.indexs = []

        # info need to read from file
        self.type = pType
        self.overflowNum = overflow
        self.overflowPage = None
        self.indexsCount = indexsCount
        self.keySize = keySize

        # for Extendible only
        self.localDepth = localDepth

    def addIndex(self, key, rid):
        self.indexs.append([key, rid])

    def searchKey(self, key):
        result = []
        for _key, _rid in self.indexs:
            if key in _key:
                result.append(_rid)

        if self.overflowPage is not None:
            return result + self.overflowPage.searchKey(key)

        return result


# helper function readPage
# given indexFile_fd, pageNum, pageSize
# return class Bucket
def readPage(index_fd, indexType, pageNum, pageSize):
    global indexPage_read_count
    indexPage_read_count+=1
    # move pointer to current position
    index_fd.seek(pageSize * pageNum, 0)
    # read page type
    pType = int.from_bytes(index_fd.read(4), "big")
    # read overflow page num
    overflow = int.from_bytes(index_fd.read(4), "big")
    # read index count
    indexCount = int.from_bytes(index_fd.read(4), "big")
    # read keySize
    keySize = int.from_bytes(index_fd.read(4), "big")

    # create page class
    bucket = Bucket(pType, overflow, indexCount, keySize)

    # skip 8 bytes
    if indexType == 1:
        pass
    # read indexs
    for i in range(indexCount):
        key = index_fd.read(12).decode("ascii")
        rid = int.from_bytes(index_fd.read(4), "big")
        bucket.addIndex(key, rid)

    # if there is a overflow page, read it recursively
    if bucket.overflowNum != 0:
        bucket.overflowPage = readPage(index_fd, indexType, bucket.overflowNum, pageSize)

    return bucket

def loadStaticIndex(index_fd, pageSize, pPageCount):
    primaryTable = []
    for i in range(1, pPageCount+1):
        primaryTable.append(readPage(index_fd, 0, i, pageSize))

    return primaryTable

# main search algorithm for static hashing
# takes in the index file and target key
# return the list of rid which found matched in the indexs
def searchKeyStatic(index_fd, pageSize, primaryBucketCount, key):
    bucketIdx = md5(key)%(primaryBucketCount)
    pageNum = bucketIdx + 1
    bucket = readPage(index_fd, 0, pageNum, pageSize)
    result = bucket.searchKey(key)

    return result

# main search algorithm for Extendible hashing
# takes in the index file and target key
# return the list of rid which found matched in the indexs
def searchKeyEntendible(indexFile_fd, pageSize, primaryBucketCount, key):
    # read out additional info
    globalDepth= int.from_bytes(indexFile_fd.read(4), "big")
    initBucketNum = int.from_bytes(indexFile_fd.read(4), "big")

    # hash
    bucketIdx = md5(key)%(((2**globalDepth) * initBucketNum))

    pageNum = bucketIdx + 1
    bucket = readPage(indexFile_fd, 2, pageNum, pageSize)

    result = bucket.searchKey(key)

    return result

# main search algorithm for linear hashing
# takes in the index file and target key
# return the list of rid which found matched in the indexs
def searchKeyLinear(indexFile_fd, pageSize, primaryBucketCount, key):
    # read out additional info
    splitPointer = int.from_bytes(indexFile_fd.read(4), "big")
    h_depth = int.from_bytes(indexFile_fd.read(4), "big")
    curr_len = int.from_bytes(indexFile_fd.read(4), "big")
    initBucketNum = int.from_bytes(indexFile_fd.read(4), "big")

    # hash
    bucketIdx = md5(key)%(((2**h_depth) * initBucketNum))
    if bucketIdx not in range(splitPointer, curr_len):
        bucketIdx = md5(key)%(((2**(h_depth+1)) * initBucketNum))

    pageNum = bucketIdx + 1
    bucket = readPage(indexFile_fd, 2, pageNum, pageSize)

    result = bucket.searchKey(key)

    return result

if __name__ == '__main__':
    # Input parameters
    inDB = sys.argv[1]
    indexFile = sys.argv[2]
    field = int(sys.argv[3])
    value = sys.argv[4]

    key_padding = 12-len(value)
    key = value + '\0'*key_padding

    # open file
    inDB_fd = open(inDB, "r")
    indexFile_fd = open(indexFile, "rb")

    # read out page size
    pageSize = int.from_bytes(indexFile_fd.read(4), "big")

    # read out index type
    indexType = int.from_bytes(indexFile_fd.read(4), "big")

    # read primary page count
    primaryPageCount = int.from_bytes(indexFile_fd.read(4), "big")

    result = [] # this is the list to hold matched rid

    # depend on index type, use different search method
    if indexType == 0:
        # loadStaticIndex(indexFile_fd, pageSize, primaryPageCount)
        result = searchKeyStatic(indexFile_fd, pageSize, primaryPageCount, key)
    elif indexType == 1:
        result = searchKeyEntendible(indexFile_fd, pageSize, primaryPageCount, key)
    elif indexType == 2:
        result = searchKeyLinear(indexFile_fd, pageSize, primaryPageCount, key)

    #
    row_per_page = pageSize//64
    pagelist = []

    # Print out results
    recordCount = 0
    print("FirstName   ,LastName      ,Email")
    for rid in result:
        recordCount += 1
        # move pointer to the current position
        pagelist.append(rid//row_per_page)
        inDB_fd.seek(rid * 64, 0)
        buf = inDB_fd.read(64)
        fName = buf[:12]
        lName = buf[12:26]
        email = buf[26:]
        print(fName+','+lName+','+email)
    pagelist = set(pagelist)
    dataPage_read_count = len(pagelist)
    print("Total record found: %d records"%recordCount)
    print()
    print("Summery:")
    print("Number of index page(primary + overflow) read: "+str(indexPage_read_count))
    print("Number of data(main db) page read: "+str(dataPage_read_count))

    # close file
    inDB_fd.close()
    indexFile_fd.close()