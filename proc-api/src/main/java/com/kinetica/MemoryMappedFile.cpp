#pragma GCC diagnostic ignored "-Wunused-function"

#ifndef __linux__
#error Due to native components, the Kinetica Proc API can only be compiled on Linux.
#endif

#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

class MemoryMappedFile
{
    public:
        MemoryMappedFile() :
            m_file(-1),
            m_writable(false),
            m_size(0),
            m_data(NULL),
            m_pos(0)
        {
        }

        ~MemoryMappedFile()
        {
            unmap();
        }

        void map(const std::string& path, bool writable, std::size_t size)
        {
            unmap();
            m_file = open(path.c_str(), writable ? O_RDWR | O_CREAT : O_RDONLY, 0);

            if (m_file == -1)
            {
                throw std::runtime_error("Could not open map file: " + std::string(std::strerror(errno)));
            }

            m_writable = writable;
            remap(size);
        }

        void remap(std::size_t size)
        {
            if (m_file == -1)
            {
                throw std::runtime_error("File not mapped");
            }

            if (size == (std::size_t)-1)
            {
                struct stat st;

                if (fstat(m_file, &st) != 0)
                {
                    int err = errno;
                    unmap();
                    throw std::runtime_error("Could not get size of map file: " + std::string(std::strerror(err)));
                }

                size = st.st_size;
            }
            else if (m_writable)
            {
                if (ftruncate(m_file, size) != 0)
                {
                    int err = errno;
                    unmap();
                    throw std::runtime_error("Could not set size of map file: " + std::string(std::strerror(err)));
                }
            }

            void* data;

            if (size == 0)
            {
                if (m_size > 0)
                {
                    munmap(m_data, m_size);
                    m_size = 0;
                    m_data = NULL;
                }

                return;
            }
            else if (m_size == 0)
            {
                data = mmap(NULL, size, m_writable ? PROT_READ | PROT_WRITE : PROT_READ, MAP_SHARED, m_file, 0);
            }
            else
            {
                data = mremap(m_data, m_size, size, MREMAP_MAYMOVE);
            }

            if (data == MAP_FAILED)
            {
                int err = errno;
                unmap();
                throw std::runtime_error("Could not map file: " + std::string(std::strerror(err)));
            }

            m_data = data;
            m_size = size;
        }

        void unmap()
        {
            if (m_file != -1)
            {
                if (m_size > 0)
                {
                    munmap(m_data, m_size);
                    m_size = 0;
                    m_data = NULL;
                }

                close(m_file);
                m_file = -1;
                m_writable = false;
                m_pos = 0;
            }
        }

        std::size_t getSize() const
        {
            return m_size;
        }

        std::size_t getPos() const
        {
            return m_pos;
        }

        template<typename T>
        const T& read()
        {
            ensure(sizeof(T));
            T* result = (T*)&((char*)m_data)[m_pos];
            m_pos += sizeof(T);
            return *result;
        }

        void read(void* value, const std::size_t length) {
            ensure(length);
            std::memcpy(value, &((char*)m_data)[m_pos], length);
            m_pos += length;
        }

        template<typename T>
        const T& read(const std::size_t pos) const
        {
            return *(T*)&((char*)m_data)[pos];
        }

        std::size_t readCharN(const std::size_t pos, void* value, const std::size_t size) const
        {
            bool found = false;
            std::size_t length = 0;

            for (std::size_t i = 0; i < size; ++i)
            {
                std::size_t j = size - i - 1;
                char temp = ((char*)m_data)[pos + i];

                if (!found && temp != 0)
                {
                    found = true;
                    length = j;
                }

                ((char*)value)[j] = temp;
            }

            return found ? length + 1 : 0;
        }

        void read(const std::size_t pos, void* value, const std::size_t length) const
        {
            std::memcpy(value, &((char*)m_data)[pos], length);
        }

        template<typename T>
        void write(const T& value)
        {
            ensure(sizeof(T));
            T* result = (T*)&((char*)m_data)[m_pos];
            m_pos += sizeof(T);
            *result = value;
        }

        void write(const void* value, const std::size_t length)
        {
            ensure(length);
            std::memcpy(&((char*)m_data)[m_pos], value, length);
            m_pos += length;
        }

        template<typename T>
        void write(const std::size_t pos, const T& value)
        {
            *(T*)&((char*)m_data)[pos] = value;
        }

        void write(const std::size_t pos, void* value, const std::size_t length)
        {
            std::memcpy(&((char*)m_data)[pos], value, length);
        }

        void writeCharN(const std::size_t pos, void* value, const std::size_t length, const std::size_t size)
        {
            for (std::size_t i = 0; i < size; ++i)
            {
                if (i < length)
                {
                    ((char*)m_data)[pos + size - i - 1] = ((char*)value)[i];
                }
                else
                {
                    ((char*)m_data)[pos + size - i - 1] = 0;
                }
            }
        }

        void truncate()
        {
            remap(m_pos);
        }

    private:
        static std::size_t MEM_PAGE_SIZE;

        int m_file;
        bool m_writable;
        std::size_t m_size;
        void* m_data;
        std::size_t m_pos;

        MemoryMappedFile(const MemoryMappedFile&);
        MemoryMappedFile& operator=(const MemoryMappedFile&);

        void ensure(const std::size_t length)
        {
            if (m_pos + length > m_size)
            {
                if (!m_writable)
                {
                    throw std::runtime_error("End of file reached");
                }
                else
                {
                    std::size_t minSize = m_pos + length;
                    remap(minSize + (MEM_PAGE_SIZE - (minSize % MEM_PAGE_SIZE)));
                }
            }
        }
};

std::size_t MemoryMappedFile::MEM_PAGE_SIZE = std::labs(sysconf(_SC_PAGESIZE));
