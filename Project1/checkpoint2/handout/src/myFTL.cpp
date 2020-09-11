#include "common.h"
#include "myFTL.h"
#include "math.h"
#include "unordered_map"

template <typename PageType>
class MyFTL : public FTLBase<PageType> {
private:
    size_t ssd_size;
    size_t package_size;
    size_t die_size;
    size_t plane_size;
    size_t block_size;
    size_t block_erase_count;
    size_t op;

    size_t overall_block_capacity;
    size_t overall_pages_capacity;
    size_t available_block_number;
    size_t available_pages_number;
    size_t overprovision_block_number;
    size_t overprovision_pages_number;
public:
    /*
     * Constructor
     */
    MyFTL(const ConfBase *conf) {
    	/* Number of packages in a ssd */
	ssd_size = conf->GetSSDSize();
	/* Number of dies in a package */
	package_size = conf->GetPackageSize();
	/* Number of planes in a die */
	die_size = conf->GetDieSize();
	/* Number of blocks in a plane */
	plane_size = conf->GetPlaneSize();
	/* Number of pages in a block */
	block_size = conf->GetBlockSize();
	/* Maximum number a block can be erased */
	block_erase_count = conf->GetBlockEraseCount();
    	/* Overprovioned blocks as a percentage of total number of blocks */
	op = conf->GetOverprovisioning();

	printf("SSD Configuration: %zu, %zu, %zu, %zu, %zu\n",
		ssd_size, package_size, die_size, plane_size, block_size);
	printf("Max Erase Count: %zu, Overprovisioning: %zu%%\n",
		block_erase_count, op);
	overall_block_capacity = ssd_size * package_size * die_size * plane_size;
	overall_pages_capacity = overall_block_capacity * block_size;
	overprovision_block_number = (size_t) round((double) overall_block_capacity * op / 100);
	available_block_number = overall_block_capacity - overprovision_block_number;
    available_pages_number = available_block_number * block_size;
	overprovision_pages_number = overall_pages_capacity - available_pages_number;
    printf("Overall page numbers: %zu, available_pages_number: %zu, overprovision_pages_number: %zu\n", 
        overall_pages_capacity, available_pages_number, overprovision_pages_number);
	printf("Overall_block_capacity: %zu, available_block_number: %zi, overprovision_block_number: %zu\n", 
        overall_block_capacity, available_block_number, overprovision_block_number);
    
    long physical_page_index = 0;

	}	

    /*
     * Destructor - Plase keep it as virtual to allow destroying the
     *              object with base type pointer
     */
    virtual ~MyFTL() {
    }

    /*
     * ReadTranslate() - Translates read address
     *
     * This function translates a physical LBA into an Address object that will
     * be used as the target address of the read operation.
     *
     * If you need to issue extra operations, please use argument func to
     * interact with class Controller
     */
    std::pair<ExecState, Address>
    ReadTranslate(size_t lba, const ExecCallBack<PageType> &func) {
        (void) lba;
        (void) func;
        if (lba >= overall_pages_capacity) {
            return std::make_pair(ExecState::FAILURE, Address(0, 0, 0, 0, 0));
        }
        return std::make_pair(ExecState::SUCCESS, Address(0, 0, 0, 0, 0));
    }

    /*
     * WriteTranslate() - Translates write address
     *
     * Please refer to ReadTranslate()
     */
    std::pair<ExecState, Address>
    WriteTranslate(size_t lba, const ExecCallBack<PageType> &func) {
        (void) lba;
        (void) func;
        return std::make_pair(ExecState::SUCCESS, Address(0, 0, 0, 0, 0));
    }

    /*
     * Optionally mark a LBA as a garbage.
     */
    ExecState
    Trim(size_t lba, const ExecCallBack<PageType>& func) {
        (void) lba;
        (void) func;
        return ExecState::SUCCESS;
    }
};

/*
 * CreateMyFTL() - Creates class MyFTL object
 *
 * You do not need to modify this
 */
FTLBase<TEST_PAGE_TYPE>* CreateMyFTL(const ConfBase *conf) {

	MyFTL<TEST_PAGE_TYPE> *ftl = new MyFTL<TEST_PAGE_TYPE>(conf);
	return static_cast<FTLBase<TEST_PAGE_TYPE>*>(ftl);
}
