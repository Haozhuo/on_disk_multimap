#ifndef DiskMultiMap_h
#define DiskMultiMap_h

#include <string>
#include "MultiMapTuple.h"
#include "BinaryFile.h"
#include <functional>


class DiskMultiMap
{
public:
    
    class Iterator
    {
    public:
        Iterator();
        Iterator(BinaryFile::Offset bucket, BinaryFile::Offset offset, BinaryFile* bf);
        bool isValid() const;
        Iterator& operator++();
        MultiMapTuple operator*();
        
    private:
        BinaryFile::Offset m_IterOffset;
        BinaryFile::Offset m_bNumber;
        BinaryFile* bf;
    };
    
    DiskMultiMap();
    ~DiskMultiMap();
    bool createNew(const std::string& filename, unsigned int numBuckets);
    bool openExisting(const std::string& filename);
    void close();
    bool insert(const std::string& key, const std::string& value, const std::string& context);
    Iterator search(const std::string& key);
    int erase(const std::string& key, const std::string& value, const std::string& context);
private:
    BinaryFile m_file;
    int m_bucketNum;
    BinaryFile::Offset m_firstOffset, deleteListOffset;
    
    struct DiskNode
    {
        char keyD[120+1];
        char valueD[120+1];
        char contextD[120+1];
        BinaryFile::Offset next;
        bool hasBeenDeleted;
        BinaryFile::Offset m_offset;
    };
    
    struct bucketNode
    {
        int number;
        BinaryFile::Offset firstNodePos;
    };
    
    int hashValue(const string& text)
    {
        std::hash<std::string> hashFunc;
        unsigned int hashNumber = hashFunc(text);
        return (hashNumber % m_bucketNum);
    }
};

#endif /* DiskMultiMap_h */
