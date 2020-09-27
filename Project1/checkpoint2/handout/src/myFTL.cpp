#include "common.h"
#include "myFTL.h"
#include "unordered_map"
#include "math.h"

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
size_t cleaning_resercation_p;
size_t gc_policy;

size_t overall_block_capacity;
size_t overall_pages_capacity;
size_t available_block_number;
size_t available_pages_number;
size_t overprovision_block_number;
size_t overprovision_pages_number;

size_t cleaning_reservation_block_number;
size_t log_reservation_block_number;
size_t upper_threshold_for_log_reservation_page_number;

std::unordered_map<size_t, size_t> first_write_lba_pba_map;
std::unordered_map<size_t, size_t> first_write_pba_lba_map;
std::unordered_map<size_t, size_t> unprovision_lba_pba_map;
/* block index in available blocks as key, page index in overprovision blocks as value*/
std::unordered_map<size_t, size_t> log_reservation_block_map;
/* Frist page in  overprovision blocks key, page in available blocks as value*/
std::unordered_map<size_t, size_t> log_reservation_to_available_page_map;

std::unordered_map<size_t, size_t> erase_record_map;
std::unordered_map<size_t, bool> just_erased_map;
// size_t physical_page_index;
size_t log_reservation_page_index;
size_t cleaning_reservation_page_index;
size_t write_index;
size_t garbage_collection_log_reservation_page_index;

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
    /* Garbage Collection policy */
    gc_policy = conf->GetGCPolicy();
    /* Cleaning -reservation blocks */
    cleaning_resercation_p = 50;
    /* Overall block number */
	overall_block_capacity = ssd_size * package_size * die_size * plane_size;
    /* Overall page number */
	overall_pages_capacity = overall_block_capacity * block_size;
    /* Block number for overprovision */
	overprovision_block_number = (size_t) round((double) overall_block_capacity * op / 100);
    /* Block number for log reservation */
    cleaning_reservation_block_number = (size_t) round((double) overprovision_block_number * cleaning_resercation_p / 100);
    /* Block number for cleaning */
    log_reservation_block_number = overprovision_block_number - cleaning_reservation_block_number;
    /* Block number exposed to users */
	available_block_number = overall_block_capacity - overprovision_block_number;
    /* page number exposed to users */
    available_pages_number = available_block_number * block_size;
    /* page number for overprovision */
	overprovision_pages_number = overall_pages_capacity - available_pages_number;
    /*upper threshold, also start of cleaning reservation page*/
    upper_threshold_for_log_reservation_page_number = available_pages_number + log_reservation_block_number * block_size;
    /* used to track next available page in lba*/
    // physical_page_index = 0;
    /* used to track next available page in log_reservation*/
    log_reservation_page_index = available_pages_number;
    /* used to track next availabel page in clearning_reservation*/
    cleaning_reservation_page_index = upper_threshold_for_log_reservation_page_number;
    /* Used to track which reservaion block has been cleaned*/
    garbage_collection_log_reservation_page_index = available_pages_number;
    printf("SSD Configuration: %zu, %zu, %zu, %zu, %zu\n",
		ssd_size, package_size, die_size, plane_size, block_size);
	printf("Max Erase Count: %zu, Overprovisioning: %zu\n", 
		block_erase_count, op);
    printf("Garbage Collection Policy %zu\n", gc_policy);
    printf("available_block_number %zu, overprovision_block_number %zu, log_reservation_block_number %zu, cleaning_reservation_block_number %zu\n", available_block_number, 
    overprovision_block_number, log_reservation_block_number, cleaning_reservation_block_number);
    write_index = 0;
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
        /* check if lba is valid*/
        if (lba >= available_pages_number) {
            return std::make_pair(ExecState::FAILURE, Address(0, 0, 0, 0, 0));
        }
        if (first_write_lba_pba_map.find(lba) == first_write_lba_pba_map.end()) {
            /* Not written in SSD yet*/
            return std::make_pair(ExecState::FAILURE, Address(0, 0, 0, 0, 0));
        } else {
            Address first_wirte_address = translatePageNumberToAddress(first_write_lba_pba_map.find(lba)->second);
            if (unprovision_lba_pba_map.find(lba) == unprovision_lba_pba_map.end()) {
                /* Just read value that have been first witten */
                return std::make_pair(ExecState::SUCCESS, first_wirte_address);
            } else {
                /* Just return valid value*/
                Address overprovision_page_address = translatePageNumberToAddress(unprovision_lba_pba_map.find(lba)->second);
                return std::make_pair(ExecState::SUCCESS, overprovision_page_address);
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
        (void) func;
        write_index++;
        printf("Write index %zu, lba %zu\n", write_index, lba);
        if (lba >= available_pages_number) {
             /* check if lba is valid */
            return std::make_pair(ExecState::FAILURE, Address(0, 0, 0, 0, 0));
        }
        if (first_write_lba_pba_map.find(lba) == first_write_lba_pba_map.end()) {
            /* First write*/
            Address new_address = translatePageNumberToAddress(lba);
            first_write_lba_pba_map[lba] = lba;
            first_write_pba_lba_map[lba] = lba;
            // physical_page_index++;
            printf("Debug in first write\n");
            debug();
            return std::make_pair(ExecState::SUCCESS, new_address);
        } else {
            /* lba written before*/
            size_t old_physical_page_index = first_write_lba_pba_map.find(lba)->second;
            Address old_address = translatePageNumberToAddress(old_physical_page_index );
            size_t block_index = tranlateAddressToBlockIndex(old_address);
            if (log_reservation_block_map.find(block_index) == log_reservation_block_map.end()) {
                /* This block has no corresponding overprovision block, cleaning here*/
                if (log_reservation_page_index >= upper_threshold_for_log_reservation_page_number || 
                    log_reservation_to_available_page_map.find(log_reservation_page_index) != log_reservation_to_available_page_map.end()) {
                    /* overprovision block used up */
                    roundRobinGarbageCollection(func);
                    return WriteTranslate(lba, func);
                }
                /* Assign new overprovision block */
                Address new_block_addresss = translatePageNumberToAddress(log_reservation_page_index);
                unprovision_lba_pba_map[lba] = log_reservation_page_index;
                log_reservation_block_map[block_index] = log_reservation_page_index;
                log_reservation_to_available_page_map[log_reservation_page_index] = old_physical_page_index;
                log_reservation_page_index += block_size;
                printf("Debug in first write to new overprovision block\n");
                debug();
                return std::make_pair(ExecState::SUCCESS, new_block_addresss);
            } else {
                /* This block has corresponding overprovision block*/
                size_t overprovision_page_index = log_reservation_block_map.find(block_index)->second;
                Address overprovision_page_address = translatePageNumberToAddress(overprovision_page_index);
                size_t block_index_in_overprovision = tranlateAddressToBlockIndex(overprovision_page_address);
                if (overprovision_page_address.page >= block_size - 1) {
                    /* corresponding overprovision block is full*/
                    cleaning_reservation_page_index = cleaningForFullLogReservationBlock(cleaning_reservation_page_index, block_index, func, overprovision_page_address);
                    if (cleaning_reservation_page_index >= overall_pages_capacity) {
                        cleaning_reservation_page_index = upper_threshold_for_log_reservation_page_number;
                    }
                    printf("Debug after erase in corresponding block\n");
                    debug();
                    return WriteTranslate(lba, func);
                } else {
                    /* corresponding overprovision block is not full*/
                    /* After Erase */
                    if (overprovision_page_address.page == 0 && just_erased_map.find(block_index_in_overprovision) != just_erased_map.end() && just_erased_map.find(block_index_in_overprovision)->second == true) {
                        just_erased_map[block_index_in_overprovision] = false;
                        Address new_overprovision_page_address = Address(overprovision_page_address.package, overprovision_page_address.die, 
                            overprovision_page_address.plane, overprovision_page_address.block, overprovision_page_address.page);
                        unprovision_lba_pba_map[lba] = overprovision_page_index;
                        log_reservation_block_map[block_index] = overprovision_page_index;
                        printf("Debug in writing to unfull block in overprovision\n");
                        debug();
                        return std::make_pair(ExecState::SUCCESS, new_overprovision_page_address);
                    } else {
                        Address new_overprovision_page_address = Address(overprovision_page_address.package, overprovision_page_address.die, 
                            overprovision_page_address.plane, overprovision_page_address.block, overprovision_page_address.page + 1);
                        unprovision_lba_pba_map[lba] = overprovision_page_index + 1;
                        log_reservation_block_map[block_index] = overprovision_page_index + 1;
                        printf("Debug in writing to unfull block in overprovision\n");
                        debug();
                        return std::make_pair(ExecState::SUCCESS, new_overprovision_page_address);
                    }
                }
            }
        }
        return std::make_pair(ExecState::SUCCESS,Address(0, 0, 0, 0, 0));
    }

    void roundRobinGarbageCollection(const ExecCallBack<PageType> &func) {
        size_t start_page_for_overprovision_block = garbage_collection_log_reservation_page_index;
        size_t page_in_available_block = log_reservation_to_available_page_map.find(start_page_for_overprovision_block)->second;
        size_t curr_cleaning_page_index = cleaning_reservation_page_index;
        size_t old_block_index = tranlateAddressToBlockIndex(translatePageNumberToAddress(page_in_available_block));
        size_t start_page_for_original_block = old_block_index * block_size;
        Address overprovision_page_address = translatePageNumberToAddress(start_page_for_overprovision_block);
        std::unordered_map<size_t, size_t> copy_map;
        std::unordered_map<size_t, size_t> corresponding_map;
        printf("Enter Garbage Collection\n");
        for (size_t i = start_page_for_original_block ; i < start_page_for_original_block + block_size; i++) {
            if (first_write_pba_lba_map.find(i) != first_write_pba_lba_map.end()) {
                size_t lba = first_write_pba_lba_map.find(i)->second;
                if (unprovision_lba_pba_map.find(lba) == unprovision_lba_pba_map.end()) {
                    copy_map[curr_cleaning_page_index] = i;
                    corresponding_map[curr_cleaning_page_index] = i;
                    curr_cleaning_page_index++;
                } else {
                    size_t pba_in_overprovision = unprovision_lba_pba_map.find(lba)->second;
                    unprovision_lba_pba_map.erase(lba);
                    copy_map[curr_cleaning_page_index] = pba_in_overprovision;
                    corresponding_map[curr_cleaning_page_index] = i;
                    curr_cleaning_page_index++;
                }
            }
        }
        if (copy_map.size() == 1) {
            log_reservation_block_map.erase(old_block_index);
            log_reservation_to_available_page_map.erase(start_page_for_overprovision_block);
            func(OpCode::ERASE, translatePageNumberToAddress(start_page_for_original_block));
            for (auto it = copy_map.begin(); it !=copy_map.end(); ++it) {
                func(OpCode::READ, translatePageNumberToAddress(it->second));
                Address original = translatePageNumberToAddress(corresponding_map.find(it->first)->second);
                func(OpCode::WRITE, original);
            }
            func(OpCode::ERASE,  overprovision_page_address);
            garbage_collection_log_reservation_page_index += block_size;
            if (garbage_collection_log_reservation_page_index >= upper_threshold_for_log_reservation_page_number) {
                garbage_collection_log_reservation_page_index = available_pages_number;
            }
            if (log_reservation_page_index >= upper_threshold_for_log_reservation_page_number) log_reservation_page_index = available_pages_number;
        } else {
            log_reservation_block_map.erase(old_block_index);
            log_reservation_to_available_page_map.erase(start_page_for_overprovision_block);
            for (auto it = copy_map.begin(); it !=copy_map.end(); ++it) {
                func(OpCode::READ, translatePageNumberToAddress(it->second));
                func(OpCode::WRITE, translatePageNumberToAddress(it->first));
            }
            func(OpCode::ERASE, overprovision_page_address);
            func(OpCode::ERASE, translatePageNumberToAddress(start_page_for_original_block));
            for (auto it = corresponding_map.begin(); it !=corresponding_map.end(); ++it) {
                func(OpCode::READ, translatePageNumberToAddress(it->first));
                func(OpCode::WRITE, translatePageNumberToAddress(it->second));
            }
            func(OpCode::ERASE, translatePageNumberToAddress(curr_cleaning_page_index));
            garbage_collection_log_reservation_page_index += block_size;
            cleaning_reservation_page_index += block_size;
            if (cleaning_reservation_page_index >= overall_pages_capacity) {
                cleaning_reservation_page_index = upper_threshold_for_log_reservation_page_number;
            }
            if (log_reservation_page_index >= upper_threshold_for_log_reservation_page_number) log_reservation_page_index = available_pages_number;
        }

    }

    size_t cleaningForFullLogReservationBlock(size_t cleaning_reservation_page_index, size_t old_block_index, const ExecCallBack<PageType> &func, Address overprovision_page_address) {
        size_t start_page_for_original_block = old_block_index * block_size;
        size_t start_page_for_overprovision_block = tranlateAddressToBlockIndex(overprovision_page_address) * block_size;
        size_t curr_cleaning_page_index = cleaning_reservation_page_index;
        std::unordered_map<size_t, size_t> copy_map;
        std::unordered_map<size_t, size_t> corresponding_map;
        printf("Enter Erase\n");
        for (size_t i = start_page_for_original_block ; i < start_page_for_original_block + block_size; i++) {
            if (first_write_pba_lba_map.find(i) != first_write_pba_lba_map.end()) {
                size_t lba = first_write_pba_lba_map.find(i)->second;
                if (unprovision_lba_pba_map.find(lba) == unprovision_lba_pba_map.end()) {
                    copy_map[curr_cleaning_page_index] = i;
                    corresponding_map[curr_cleaning_page_index] = i;
                    curr_cleaning_page_index++;
                } else {
                    size_t pba_in_overprovision = unprovision_lba_pba_map.find(lba)->second;
                    unprovision_lba_pba_map.erase(lba);
                    copy_map[curr_cleaning_page_index] = pba_in_overprovision;
                    corresponding_map[curr_cleaning_page_index] = i;
                    curr_cleaning_page_index++;
                }
            }
        }
        if (copy_map.size() == 1) {
            log_reservation_block_map[old_block_index] = start_page_for_overprovision_block;
            func(OpCode::ERASE, translatePageNumberToAddress(start_page_for_original_block));
            for (auto it = copy_map.begin(); it !=copy_map.end(); ++it) {
                func(OpCode::READ, translatePageNumberToAddress(it->second));
                Address original = translatePageNumberToAddress(corresponding_map.find(it->first)->second);
                func(OpCode::WRITE, original);
            }
            func(OpCode::ERASE, overprovision_page_address);
            return cleaning_reservation_page_index;
        } else {
            log_reservation_block_map[old_block_index] = start_page_for_overprovision_block;
            for (auto it = copy_map.begin(); it !=copy_map.end(); ++it) {
                func(OpCode::READ, translatePageNumberToAddress(it->second));
                func(OpCode::WRITE, translatePageNumberToAddress(it->first));
            }
            func(OpCode::ERASE, overprovision_page_address);
            func(OpCode::ERASE, translatePageNumberToAddress(start_page_for_original_block));
            for (auto it = corresponding_map.begin(); it !=corresponding_map.end(); ++it) {
                func(OpCode::READ, translatePageNumberToAddress(it->first));
                func(OpCode::WRITE, translatePageNumberToAddress(it->second));
            }
            func(OpCode::ERASE, translatePageNumberToAddress(curr_cleaning_page_index));
            cleaning_reservation_page_index += block_size;
            size_t block_index_in_overprovision = tranlateAddressToBlockIndex(overprovision_page_address);
            just_erased_map[block_index_in_overprovision] = true;
            return cleaning_reservation_page_index;
        }
    }
     /* 
     * Thanslate address to physical page index
     */
    size_t translateAddressToPageNumber(Address address) {
        size_t ans = 0;
        ans += address.page;
        ans += address.block * block_size;
        ans += address.plane * plane_size * block_size;
        ans += address.die * die_size * plane_size * block_size;
        ans += address.package * package_size * die_size * plane_size * block_size;
        return ans;
    }

    /* 
     * Thanslate physical page index to address 
     */
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
        return address;
    }

    /* 
     * Compute corrspoinding block index based on address
     */
    size_t tranlateAddressToBlockIndex(Address address) {
        size_t ans = 0;
        ans += address.block;
        ans += address.plane * plane_size;
        ans += address.die * die_size * plane_size;
        ans += address.package * package_size * die_size * plane_size;
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

    void debug() {
        printf("Debug first_write_lba_pba_map\n");
        for (auto it = first_write_lba_pba_map.begin(); it !=first_write_lba_pba_map.end(); ++it) {
            printf("lba %zu, pga %zu \n",it->first, it->second);
        }
        printf("************************************\n");
        printf("unprovision_lba_pba_map\n");
        for (auto it = unprovision_lba_pba_map.begin(); it != unprovision_lba_pba_map.end(); ++it) {
            printf("lba %zu, pga %zu \n", it->first, it->second);
        }
        printf("************************************\n");
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
