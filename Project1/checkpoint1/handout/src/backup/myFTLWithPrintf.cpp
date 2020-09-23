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

    std::unordered_map<size_t, Address> first_write_lba_pba_map;
    std::unordered_map<size_t, Address> unprovision_lba_pba_map;
    std::unordered_map<size_t, Address> log_reservation_block_map;
    size_t physical_page_index;
    size_t overprovision_page_index;

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
    
    physical_page_index = 0;
    overprovision_page_index = available_pages_number;
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
        if (lba >= available_pages_number) {
            return std::make_pair(ExecState::FAILURE, Address(0, 0, 0, 0, 0));
        }
        if (first_write_lba_pba_map.find(lba) == first_write_lba_pba_map.end()) {
            return std::make_pair(ExecState::FAILURE, Address(0, 0, 0, 0, 0));
        } else {
            Address first_wirte_address = first_write_lba_pba_map.find(lba)->second;
            if (unprovision_lba_pba_map.find(lba) == unprovision_lba_pba_map.end()) {
                return std::make_pair(ExecState::SUCCESS, first_wirte_address);
            } else {
                Address overprovision_page_address = unprovision_lba_pba_map.find(lba)->second;
                if (overprovision_page_address.page == block_size) {
                    return std::make_pair(ExecState::FAILURE, Address(0, 0, 0, 0, 0));
                } else {
                    return std::make_pair(ExecState::SUCCESS, overprovision_page_address);
                }
            }
        }
    }

    /*
     * WriteTranslate() - Translates write address
     *
     * Please refer to ReadTranslate()
     */
    std::pair<ExecState, Address>
    WriteTranslate(size_t lba, const ExecCallBack<PageType> &func) {
        printf("************************************************\n");
        printf("writing %zu\n", lba);
        (void) func;
        if (lba >= available_pages_number) {
            return std::make_pair(ExecState::FAILURE, Address(0, 0, 0, 0, 0));
        }
        if (first_write_lba_pba_map.find(lba) == first_write_lba_pba_map.end()) {
            // printf("1\n");
            Address new_address = translatePageNumberToAddress(physical_page_index);
            first_write_lba_pba_map[lba] = new_address;
            physical_page_index++;
            return std::make_pair(ExecState::SUCCESS, new_address);
        } else {
            Address old_address = first_write_lba_pba_map.find(lba)->second;
            size_t block_index = tranlateAddressToBlockIndex(old_address);
            if (log_reservation_block_map.find(block_index) == log_reservation_block_map.end()) {
                if (overprovision_page_index >= overall_pages_capacity - 1) {
                    // printf("1\n");
                    return std::make_pair(ExecState::FAILURE, Address(0, 0, 0, 0, 0));
                }
                Address new_block_addresss = translatePageNumberToAddress(overprovision_page_index);
                overprovision_page_index += block_size;
                unprovision_lba_pba_map[lba] = new_block_addresss;
                log_reservation_block_map[block_index] = new_block_addresss;
                return std::make_pair(ExecState::SUCCESS, new_block_addresss);
            } else {
                Address overprovision_page_address = log_reservation_block_map.find(block_index)->second;
                if (overprovision_page_address.page >= block_size - 1) {
                    printf("3\n");
                    unprovision_lba_pba_map[lba] = Address(ssd_size, package_size, die_size, plane_size, block_size); 
                    return std::make_pair(ExecState::FAILURE, Address(0, 0, 0, 0, 0));
                } else {
                    printf("4\n");
                    Address new_overprovision_page_address = Address(overprovision_page_address.package, overprovision_page_address.die, 
                    overprovision_page_address.plane, overprovision_page_address.block, overprovision_page_address.page + 1);
                    unprovision_lba_pba_map[lba] = new_overprovision_page_address;
                    log_reservation_block_map[block_index] = new_overprovision_page_address;
                    return std::make_pair(ExecState::SUCCESS, new_overprovision_page_address);
                }
            }
        }
    }

    Address translatePageNumberToAddress(size_t ppa) {
        size_t tmp = ppa;
        size_t package_index = ppa / (package_size * die_size * plane_size * block_size);
        tmp -= package_index * (package_size * die_size * plane_size * block_size);
        size_t die_index =  tmp / (die_size * plane_size * block_size);
        tmp -= die_index * (die_size * plane_size * block_size);
        size_t plane_index = tmp / (plane_size * block_size);
        tmp -= plane_index * (plane_size * block_size);
        size_t block_index = tmp / block_size;
        size_t page_index = tmp % block_size;
        Address address = Address(package_index, die_index, plane_index, block_index, page_index);
        printf("Debug translatePageNumberToAddress %zu\n", ppa);
        printf("Address is Package: %d, Die %d, Plane: %d, Block: %d, Page: %d\n", address.package, address.die, address.plane, address.block, address.page);
        return address;
    }

    size_t tranlateAddressToBlockIndex(Address address) {
        size_t ans = 0;
        ans += address.block;
        ans += address.plane * plane_size;
        ans += address.die * die_size * plane_size;
        ans += address.package * package_size * die_size * plane_size;
        printf("Debug tranlateAddressToBlockIndex\n");
        printf("Address is Package: %d, Die %d, Plane: %d, Block: %d, Page: %d\n", address.package, address.die, address.plane, address.block, address.page);
        printf("BlockIndex %zu\n", ans);
        return ans;
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
