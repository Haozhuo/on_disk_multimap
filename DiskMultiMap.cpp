#include "DiskMultiMap.h"
#include "BinaryFile.h"

DiskMultiMap::DiskMultiMap():m_bucketNum(0),m_firstOffset(0),deleteListOffset(0)
{}

DiskMultiMap::~DiskMultiMap()
{
    if(m_file.isOpen())
        m_file.close();
}

bool DiskMultiMap::openExisting(const std::string& filename)
{
    if(m_file.isOpen())
        m_file.close();
    
    if(!m_file.openExisting(filename))
        return false;
    
    m_file.read(m_bucketNum, 0);
    m_file.read(m_firstOffset, sizeof(int));
    m_file.read(deleteListOffset, sizeof(int) + sizeof(BinaryFile::Offset));
    
    return true;
}

void DiskMultiMap::close()
{
    if(m_file.isOpen())
        m_file.close();
}

bool DiskMultiMap::createNew(const std::string& filename, unsigned int numBuckets)
{
    if(m_file.isOpen())
        m_file.close();
    
    if(!m_file.createNew(filename))
        return false;
    
    m_bucketNum = numBuckets;
    
    deleteListOffset = 0;
    m_file.write(m_bucketNum, 0);
    m_file.write(m_firstOffset, sizeof(int));
    m_file.write(deleteListOffset, sizeof(int) + sizeof(BinaryFile::Offset));
    
    int header = sizeof(int) + 2 * sizeof(BinaryFile::Offset);
    m_firstOffset = header + numBuckets * sizeof(bucketNode);
    
    int i = 0;
    for(; i < numBuckets; i++)
    {
        bucketNode temp;
        temp.number = i;
        temp.firstNodePos = 0;
        m_file.write(temp, header + i * sizeof(bucketNode));
    }
    
    return true;
}

bool DiskMultiMap::insert(const std::string& key, const std::string& value, const std::string& context)
{
    if(key.size() > 120 || value.size() > 120 || context.size() > 120)
        return false;
    
    DiskNode newNode;
    strcpy(newNode.keyD,key.c_str());
    strcpy(newNode.valueD, value.c_str());
    strcpy(newNode.contextD, context.c_str());
    newNode.hasBeenDeleted = false;
    
    bucketNode tempBucketNode;
    int myBucketNumber = hashValue(key);
    int header = sizeof(int) + 2 * sizeof(BinaryFile::Offset);
    m_file.read(tempBucketNode, header + myBucketNumber * sizeof(bucketNode));
    
    //no resuable space
    if(deleteListOffset == 0)
    {
        //no element condition
        if(tempBucketNode.firstNodePos == 0)    //no element in this list
        {
            newNode.next = 0;
            newNode.m_offset = m_file.fileLength();
            tempBucketNode.firstNodePos = m_file.fileLength();
            m_file.write(tempBucketNode, header + myBucketNumber * sizeof(bucketNode));
            m_file.write(newNode, m_file.fileLength());
            return true;
        }
        //has element condition
        else
        {
            newNode.next = tempBucketNode.firstNodePos;
            newNode.m_offset = m_file.fileLength();
            tempBucketNode.firstNodePos = m_file.fileLength();
            m_file.write(tempBucketNode, header + myBucketNumber * sizeof(bucketNode));
            m_file.write(newNode, m_file.fileLength());
            return true;
        }
    }
    
    else    //has resuable space
    {
        if(tempBucketNode.firstNodePos == 0)    //first item
        {
            DiskNode nextDeleted;
            m_file.read(nextDeleted, deleteListOffset);
            BinaryFile::Offset secondDelete = nextDeleted.next;
            
            newNode.next = 0;
            newNode.m_offset = deleteListOffset;
            m_file.write(newNode, deleteListOffset);
            tempBucketNode.firstNodePos = deleteListOffset;
            m_file.write(tempBucketNode, header + myBucketNumber * sizeof(bucketNode));
            deleteListOffset = secondDelete;
            return true;
        }
        
        else    //not the first item
        {
            DiskNode nextDeleted;
            m_file.read(nextDeleted, deleteListOffset);
            BinaryFile::Offset secondDelete = nextDeleted.next;
            
            newNode.next = tempBucketNode.firstNodePos;
            newNode.m_offset = deleteListOffset;
            m_file.write(newNode, deleteListOffset);
            tempBucketNode.firstNodePos = deleteListOffset;
            m_file.write(tempBucketNode, header + myBucketNumber * sizeof(bucketNode));
            deleteListOffset = secondDelete;
            return true;

        }
    }
    
    return false;
}

int DiskMultiMap::erase(const std::string& key, const std::string& value, const std::string& context)
{
    if(key.size() > 120 || value.size() > 120 || context.size() > 120)
        return 0;
    
    DiskNode compDiskNode;
    DiskNode prevDiskNode;
    bucketNode tempBucketNode;
    int numberDeleted = 0;
    
    int myBucketNumber = hashValue(key);
    int header = sizeof(int) + 2 * sizeof(BinaryFile::Offset);
    m_file.read(tempBucketNode, header + myBucketNumber * sizeof(bucketNode));
    
    if(tempBucketNode.firstNodePos == 0)
        return numberDeleted;
    
    m_file.read(compDiskNode, tempBucketNode.firstNodePos);
    
    while(compDiskNode.next != 0)
    {
        if(strcmp(compDiskNode.keyD, key.c_str()) == 0 && strcmp(compDiskNode.valueD, value.c_str()) == 0 && strcmp(compDiskNode.contextD, context.c_str()) == 0)
        {
            if(tempBucketNode.firstNodePos == compDiskNode.m_offset)    //first item
            {
                BinaryFile::Offset first = tempBucketNode.firstNodePos;
                tempBucketNode.firstNodePos = compDiskNode.next;
                m_file.write(tempBucketNode, header + myBucketNumber * sizeof(bucketNode));
                compDiskNode.next = deleteListOffset;
                m_file.write(compDiskNode, compDiskNode.m_offset);
                deleteListOffset = first;
                numberDeleted++;
                m_file.read(compDiskNode, tempBucketNode.firstNodePos);
            }
            
            else
            {
                prevDiskNode.next = compDiskNode.next;
                m_file.write(prevDiskNode, prevDiskNode.m_offset);
                compDiskNode.next = deleteListOffset;
                m_file.write(compDiskNode, compDiskNode.m_offset);
                deleteListOffset = compDiskNode.m_offset;
                numberDeleted++;
                m_file.read(compDiskNode, prevDiskNode.next);
            }
        }
        
        else
        {
            m_file.read(prevDiskNode, compDiskNode.m_offset);
            m_file.read(compDiskNode, compDiskNode.next);
        }
    }
    
    if(strcmp(compDiskNode.keyD, key.c_str()) == 0 && strcmp(compDiskNode.valueD, value.c_str()) == 0 && strcmp(compDiskNode.contextD, context.c_str()) == 0)
    {
        
        if(tempBucketNode.firstNodePos == compDiskNode.m_offset)    //first item
        {
            BinaryFile::Offset first = tempBucketNode.firstNodePos;
            tempBucketNode.firstNodePos = compDiskNode.next;
            m_file.write(tempBucketNode, header + myBucketNumber * sizeof(bucketNode));
            compDiskNode.next = deleteListOffset;
            m_file.write(compDiskNode, compDiskNode.m_offset);
            deleteListOffset = first;
            numberDeleted++;
        }
        
        else
        {
            prevDiskNode.next = compDiskNode.next;
            m_file.write(prevDiskNode, prevDiskNode.m_offset);
            compDiskNode.next = deleteListOffset;
            m_file.write(compDiskNode, compDiskNode.m_offset);
            deleteListOffset = compDiskNode.m_offset;
            numberDeleted++;
        }

    }
    
    return numberDeleted;
}

DiskMultiMap::Iterator::Iterator()
{
    m_bNumber = -1;
    m_IterOffset = 0;
}

DiskMultiMap::Iterator::Iterator(BinaryFile::Offset bucket, BinaryFile::Offset offset, BinaryFile* file):m_bNumber(bucket),bf(file),m_IterOffset(offset)
{}

bool DiskMultiMap::Iterator::isValid() const
{
    return !(m_IterOffset == 0);
}

DiskMultiMap::Iterator& DiskMultiMap::Iterator::operator++()
{
    if(!isValid())
        return *this;
    
    DiskNode tempDiskNode;
    bf->read(tempDiskNode, m_IterOffset);
    char key[120 + 1];
    BinaryFile::Offset iterpos = 0;
    strcpy(key, tempDiskNode.keyD);
    
    if(tempDiskNode.next == 0)
    {
        m_IterOffset = 0;
        return *this;
    }
    
    while(tempDiskNode.next != 0)
    {
        iterpos = tempDiskNode.next;
        bf->read(tempDiskNode, tempDiskNode.next);
        if(strcmp(key, tempDiskNode.keyD) == 0)
        {
            m_IterOffset = iterpos;
            return *this;
        }
    }
    
    if(strcmp(key, tempDiskNode.keyD) == 0)
    {
        m_IterOffset = iterpos;
        return *this;
    }
    
    m_IterOffset = 0;
    return *this;
}

MultiMapTuple DiskMultiMap::Iterator::operator*()
{
    MultiMapTuple temp;
    if(!isValid())
        return temp;
    
    DiskNode iterDisk;
    bf->read(iterDisk, m_IterOffset);
    temp.key = iterDisk.keyD;
    temp.value = iterDisk.valueD;
    temp.context = iterDisk.contextD;
    
    return temp;
}

DiskMultiMap::Iterator DiskMultiMap::search(const std::string& key)
{
    int myBucketNumber = hashValue(key);
    int header = sizeof(int) + 2 * sizeof(BinaryFile::Offset);
    bucketNode tempBucketNode;
    m_file.read(tempBucketNode, header + myBucketNumber * sizeof(bucketNode));
    
    if(tempBucketNode.firstNodePos == 0)
    {
        Iterator tempIter;
        return tempIter;
    }
    
    DiskNode tempDiskNode;
    m_file.read(tempDiskNode, tempBucketNode.firstNodePos);
    BinaryFile::Offset iterpos = tempBucketNode.firstNodePos;
    
    while(tempDiskNode.next != 0)
    {
        if(strcmp(tempDiskNode.keyD, key.c_str()) == 0)
        {
            Iterator tempIter(myBucketNumber, iterpos, &m_file);
            return tempIter;
        }
        
        iterpos = tempDiskNode.next;
        m_file.read(tempDiskNode, tempDiskNode.next);
    }
    
    if(strcmp(tempDiskNode.keyD, key.c_str()) == 0)
    {
        Iterator tempIter(myBucketNumber, iterpos, &m_file);
        return tempIter;
    }
    
    Iterator tempIter;
    return tempIter;
}




