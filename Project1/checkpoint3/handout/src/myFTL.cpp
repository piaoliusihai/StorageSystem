#include "common.h"
#include "myFTL.h"
#include "unordered_map"
#include "math.h"
#include <time.h>

template <typename PageType>
class MyFTL : public FTLBase<PageType> {

private:
/* Number of packages in a ssd */
size_t ssd_size;
/* Number of dies in a package */
size_t package_size;
/* Number of planes in a die */
size_t die_size;
/* Number of blocks in a plane */
size_t plane_size;
/* Number of pages in a block */
size_t block_size;
/* Maximum number a block can be erased */
size_t block_erase_count;
/* Percentage of overprovioned blocks in total number of blocks */
size_t op;
/* Cleaning-reservation blocks percentage in total number of overprovioned blocks*/
size_t cleaning_resercation_p;
/* Garbage Collection policy */
size_t gc_policy;
/* All the block number in this ssd */
size_t overall_block_capacity;
/* All the page number in this ssd */
size_t overall_pages_capacity;
/* All the data block number in this ssd */
size_t available_block_number;
/* All the page number int data block in this ssd */
size_t available_pages_number;
/* All the overprovision block number in this ssd */
size_t overprovision_block_number;
/* All the page number in overprovision block in this ssd */
size_t overprovision_pages_number;
/* All the page number used for log reservation in overprovision block in this ssd */
size_t log_reservation_block_number;
/* All the page number used for cleaning in overprovision block in this ssd*/
size_t cleaning_reservation_block_number;
/* Upper threshold for log reservation page */
size_t upper_threshold_for_log_reservation_page_number;
/* Erase threshold to change corresponding block */
size_t full_cleaning_erase_threshod;
/* Given a lba, compute it's corresponding block index, then find the used pba block index with this map */
std::vector<size_t> lba_block_index_to_pba_block_index_map;
/* Given a pba block index, finding the log reservation block it's used */
std::vector<size_t> pba_data_block_index_to_log_reservation_block_index_map;
/* Given a pba page index, finding corresponding actual saved paged index */
std::vector<size_t> pba_page_index_map;
/* To record block erase times */
std::vector<size_t> erase_record_map;
/* The index is the log reservation block index, the value is the corresponding page in data blocks */
std::vector<size_t> log_reservation_to_available_page_map;
/* Log reservation page to allocate */
size_t log_reservation_page_index;
/* Page to clean */
size_t cleaning_reservation_page_index;
/* Log reservation block to clean */
size_t garbage_collection_log_reservation_page_index;

public:
    /*
     * Constructor
     */
    MyFTL(const ConfBase *conf) {
	ssd_size = conf->GetSSDSize();
	package_size = conf->GetPackageSize();
	die_size = conf->GetDieSize();
	plane_size = conf->GetPlaneSize();
	block_size = conf->GetBlockSize();
	block_erase_count = conf->GetBlockEraseCount();
	op = conf->GetOverprovisioning();
    gc_policy = 0;
    cleaning_resercation_p = 50;
	overall_block_capacity = ssd_size * package_size * die_size * plane_size;
	overall_pages_capacity = overall_block_capacity * block_size;
	overprovision_block_number = (size_t) round((double) overall_block_capacity * op / 100);
    cleaning_reservation_block_number = (size_t) round((double) overprovision_block_number * cleaning_resercation_p / 100);
    log_reservation_block_number = overprovision_block_number - cleaning_reservation_block_number;
	available_block_number = overall_block_capacity - overprovision_block_number;
    available_pages_number = available_block_number * block_size;
	overprovision_pages_number = overall_pages_capacity - available_pages_number;
    upper_threshold_for_log_reservation_page_number = available_pages_number + log_reservation_block_number * block_size;
    log_reservation_page_index = available_pages_number;
    cleaning_reservation_page_index = upper_threshold_for_log_reservation_page_number;
    garbage_collection_log_reservation_page_index = available_pages_number;
    full_cleaning_erase_threshod = block_erase_count - 2;
    lba_block_index_to_pba_block_index_map = std::vector<size_t>(available_block_number, -1);
    pba_data_block_index_to_log_reservation_block_index_map = std::vector<size_t>(available_block_number, -1);
    pba_page_index_map = std::vector<size_t>(available_pages_number, -1);
    erase_record_map = std::vector<size_t>(overall_block_capacity, 0);
    log_reservation_to_available_page_map = std::vector<size_t>(overprovision_block_number, -1);
    printf("SSD Configuration: %zu, %zu, %zu, %zu, %zu\n",
		ssd_size, package_size, die_size, plane_size, block_size);
	printf("Max Erase Count: %zu, Overprovisioning: %zu\n", 
		block_erase_count, op);
    printf("Garbage Collection Policy %zu\n", gc_policy);
    printf("available_block_number %zu, overprovision_block_number %zu, log_reservation_block_number %zu, cleaning_reservation_block_number %zu\n", available_block_number, 
    overprovision_block_number, log_reservation_block_number, cleaning_reservation_block_number);
    printf("cleaning_reservation_page_index %zu, log_reservation_page_index %zu, overall_pages_capacity %zu\n", cleaning_reservation_page_index, log_reservation_page_index, overall_pages_capacity);
    printf("cleaning_reservation_block_index %zu, log_reservation_block__index %zu, overall_pages_block_index %zu\n", getBlockIndex(cleaning_reservation_page_index), getBlockIndex(log_reservation_page_index), getBlockIndex(overall_pages_capacity));
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
        size_t lba_block_index = getBlockIndex(lba);
        /* check if the lba block has a corresponding block*/
        if (lba_block_index_to_pba_block_index_map[lba_block_index] == -1) {
            return std::make_pair(ExecState::FAILURE, Address(0, 0, 0, 0, 0));
        }
        size_t pba_block_index = lba_block_index_to_pba_block_index_map[lba_block_index];
        size_t pba_page_index = getPageIndexInCertainBlock(pba_block_index, lba);
        /* check if the pba page has a actual saved page*/
        if (pba_page_index_map[pba_page_index] == -1) {
            return std::make_pair(ExecState::FAILURE, Address(0, 0, 0, 0, 0));
        } else {
            size_t actual_page_index = pba_page_index_map[pba_page_index];
            Address actual_page_address = translatePageNumberToAddress(actual_page_index);
            return std::make_pair(ExecState::SUCCESS, actual_page_address);
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
        if (lba >= available_pages_number) {
            /* check if lba is valid */
            return std::make_pair(ExecState::FAILURE, Address(0, 0, 0, 0, 0));
        }
        size_t lba_block_index = getBlockIndex(lba);
        if (lba_block_index_to_pba_block_index_map[lba_block_index] == -1) {
            size_t pba_block_index = lba_block_index;
            lba_block_index_to_pba_block_index_map[lba_block_index] = pba_block_index;
            pba_page_index_map[lba] = lba;
            Address new_address = translatePageNumberToAddress(lba);
            return std::make_pair(ExecState::SUCCESS, new_address);
        } else {
            size_t pba_block_index = lba_block_index_to_pba_block_index_map[lba_block_index];
            size_t pba_page_index = getPageIndexInCertainBlock(pba_block_index, lba);
            if (pba_page_index_map[pba_page_index] == -1) {
                Address new_address = translatePageNumberToAddress(pba_page_index);
                pba_page_index_map[pba_page_index] = pba_page_index;
                return std::make_pair(ExecState::SUCCESS, new_address);
            } else {
                if (pba_data_block_index_to_log_reservation_block_index_map[pba_block_index] == -1) {
                    size_t log_reservation_block_index_from_zero_to_allocate = getBlockIndex(log_reservation_page_index) - available_block_number;
                    if (log_reservation_page_index >= upper_threshold_for_log_reservation_page_number || 
                        log_reservation_to_available_page_map[log_reservation_block_index_from_zero_to_allocate] != -1) {
                        if (!roundRobinGarbageCollection(func)) {
                            return std::make_pair(ExecState::FAILURE, Address(0, 0, 0, 0, 0));
                        }
                        return WriteTranslate(lba, func);
                    }
                    /* Assign new overprovision block */
                    Address new_block_addresss = translatePageNumberToAddress(log_reservation_page_index);
                    log_reservation_to_available_page_map[log_reservation_block_index_from_zero_to_allocate] = pba_page_index;
                    pba_page_index_map[pba_page_index] = log_reservation_page_index;
                    pba_data_block_index_to_log_reservation_block_index_map[pba_block_index] = log_reservation_page_index + 1;
                    log_reservation_page_index += block_size;
                    return std::make_pair(ExecState::SUCCESS, new_block_addresss);
                } else {
                    /* This block has corresponding overprovision block*/
                    size_t overprovision_page_index = pba_data_block_index_to_log_reservation_block_index_map[pba_block_index];
                    Address overprovision_page_address = translatePageNumberToAddress(overprovision_page_index);
                    // size_t block_index_in_overprovision = translateAddressToBlockIndex(overprovision_page_address);
                    size_t log_reservation_block_index_from_zero = getBlockIndex(overprovision_page_index) - available_block_number;
                    if (overprovision_page_address.page == 0 && log_reservation_to_available_page_map[log_reservation_block_index_from_zero] != -1) {
                        /* corresponding overprovision block is full*/
                        if(!cleaningForFullLogReservationBlock(lba, pba_block_index, overprovision_page_index, func)) return std::make_pair(ExecState::FAILURE, Address(0, 0, 0, 0, 0));
                        return WriteTranslate(lba, func);
                    } else {
                        pba_page_index_map[pba_page_index] = overprovision_page_index;
                        if (overprovision_page_address.page == 0) log_reservation_to_available_page_map[log_reservation_block_index_from_zero] = pba_page_index;
                        if (overprovision_page_address.page == block_size - 1) {
                            pba_data_block_index_to_log_reservation_block_index_map[pba_block_index] = getBlockIndex(overprovision_page_index) * block_size;
                        } else {
                            pba_data_block_index_to_log_reservation_block_index_map[pba_block_index] = overprovision_page_index + 1;
                        }
                        return std::make_pair(ExecState::SUCCESS, overprovision_page_address);
                    }
                }
            }
        }
    }

    /*
     * Grabage collection policy LRU Cost Benefit
     */
    bool roundRobinGarbageCollection(const ExecCallBack<PageType> &func) {
        size_t start_page_for_overprovision_block = garbage_collection_log_reservation_page_index;
        size_t page_in_available_block = log_reservation_to_available_page_map[getBlockIndex(start_page_for_overprovision_block) - available_block_number];
        size_t old_block_index = getBlockIndex(page_in_available_block);
        size_t start_page_for_original_block = old_block_index * block_size;
        if (!(notReachEraseLimit(start_page_for_original_block) && notReachEraseLimit(start_page_for_overprovision_block) && notReachEraseLimit(cleaning_reservation_page_index))) return false;
        bool usedCleaningBlock = performErase(cleaning_reservation_page_index, start_page_for_original_block, start_page_for_overprovision_block, func);
        if (usedCleaningBlock) {
            cleaning_reservation_page_index += block_size;
            if (cleaning_reservation_page_index >= overall_pages_capacity) {
                cleaning_reservation_page_index = upper_threshold_for_log_reservation_page_number;
            }
        }
        log_reservation_page_index = garbage_collection_log_reservation_page_index;
        garbage_collection_log_reservation_page_index += block_size;
        if (garbage_collection_log_reservation_page_index >= upper_threshold_for_log_reservation_page_number) {
            garbage_collection_log_reservation_page_index = available_pages_number;
        }
        pba_data_block_index_to_log_reservation_block_index_map[old_block_index] = -1;
        log_reservation_to_available_page_map[getBlockIndex(start_page_for_overprovision_block) - available_block_number] = -1;
        return true;
    }

    /*
     * Used by those garbage collection policy
     */
    bool cleaningForFullLogReservationBlock(size_t lba, size_t pba_data_block_index, size_t over_provision_log_page_index, const ExecCallBack<PageType> &func) {
        size_t start_page_for_original_block = pba_data_block_index * block_size;
        size_t block_index_in_overprovision = getBlockIndex(over_provision_log_page_index);
        size_t start_page_for_overprovision_block = block_index_in_overprovision * block_size;
        if (erase_record_map[pba_data_block_index] == full_cleaning_erase_threshod && erase_record_map[block_index_in_overprovision] == full_cleaning_erase_threshod ) {
            if (changeMappingForCleaningForFullLogReservationBlock(lba, start_page_for_original_block, start_page_for_overprovision_block, func)) {
                return true;
            }
        }
        if (!(notReachEraseLimit(start_page_for_original_block) && notReachEraseLimit(start_page_for_overprovision_block) && notReachEraseLimit(cleaning_reservation_page_index))) return false;
        bool usedCleaningBlock = performErase(cleaning_reservation_page_index, start_page_for_original_block, start_page_for_overprovision_block, func);
        if (usedCleaningBlock) {
            cleaning_reservation_page_index += block_size;
            if (cleaning_reservation_page_index >= overall_pages_capacity) {
                cleaning_reservation_page_index = upper_threshold_for_log_reservation_page_number;
            }
        }
        pba_data_block_index_to_log_reservation_block_index_map[pba_data_block_index] = start_page_for_overprovision_block;
        log_reservation_to_available_page_map[getBlockIndex(start_page_for_overprovision_block) - available_block_number] = -1;
        return true;
    }

    /* Change data block and log reservation block to unused block if reach the threshold */
    bool changeMappingForCleaningForFullLogReservationBlock(size_t lba, size_t start_page_for_original_block, size_t start_page_for_overprovision_block, const ExecCallBack<PageType> &func) {
        size_t data_block_index = getBlockIndex(start_page_for_original_block);
        size_t overprovision_block_index = getBlockIndex(start_page_for_overprovision_block);
        size_t new_data_block_index = -1, new_overprovision_block_index = -1, min_erase_times = block_erase_count;
        for (size_t i = 0; i < available_block_number; i++) {
            if (erase_record_map[i] == 0) {
                new_data_block_index = i;
                min_erase_times = -1;
                break;
            } else {
                if (min_erase_times > erase_record_map[i]) {
                    min_erase_times = erase_record_map[i];
                    new_data_block_index = i;
                }
            }
        }
        if (min_erase_times != -1) return false; 
        min_erase_times = block_erase_count;
        for (size_t i = available_block_number; i < upper_threshold_for_log_reservation_page_number / block_size; i++) {
            if (erase_record_map[i] == 0) {
                new_overprovision_block_index = i;
                min_erase_times = -1;
                break;
            } else {
                if (min_erase_times > erase_record_map[i]) {
                    min_erase_times = erase_record_map[i];
                    new_overprovision_block_index = i;
                }
            }
        }
        if (min_erase_times != -1) return false;
        for (size_t i = start_page_for_original_block; i < start_page_for_original_block + block_size; i++) {
            if (pba_page_index_map[i] != -1) {
                size_t original_loc = pba_page_index_map[i], new_loc = getPageIndexInCertainBlock(new_data_block_index, i % block_size);
                func(OpCode::READ, translatePageNumberToAddress(original_loc));
                pba_page_index_map[i] = -1;
                pba_page_index_map[new_loc] = new_loc;
                func(OpCode::WRITE, translatePageNumberToAddress(new_loc));
            }
        }
        func(OpCode::ERASE, translatePageNumberToAddress(data_block_index * block_size));
        func(OpCode::ERASE, translatePageNumberToAddress(overprovision_block_index * block_size));
        lba_block_index_to_pba_block_index_map[lba / block_size] = new_data_block_index;
        lba_block_index_to_pba_block_index_map[new_data_block_index] = data_block_index;

        pba_data_block_index_to_log_reservation_block_index_map[data_block_index] = -1;
        pba_data_block_index_to_log_reservation_block_index_map[new_data_block_index] = new_overprovision_block_index * block_size;

        log_reservation_to_available_page_map[overprovision_block_index - available_block_number] = -1;
        updateEraseEecordMap(data_block_index * block_size);
        updateEraseEecordMap(overprovision_block_index * block_size);
        return true;
    }

    /* 
     * Erase certain block.
     */
    bool performErase(size_t cleaning_reservation_page_index, size_t start_page_for_original_block, size_t start_page_for_overprovision_block, const ExecCallBack<PageType> &func) {
        size_t curr_cleaning_page_index = cleaning_reservation_page_index;
        Address overprovision_page_address = translatePageNumberToAddress(start_page_for_overprovision_block);
        std::unordered_map<size_t, size_t> copy_map;
        std::unordered_map<size_t, size_t> corresponding_map;
        bool ans = false;
        for (size_t i = start_page_for_original_block; i < start_page_for_original_block + block_size; i++) {
            if (pba_page_index_map[i] != -1) {
                copy_map[curr_cleaning_page_index] = pba_page_index_map[i];
                corresponding_map[curr_cleaning_page_index] = i;
                pba_page_index_map[i] = i;
                curr_cleaning_page_index++;
            }
        }
        if (copy_map.size() == 1) {
            func(OpCode::ERASE, translatePageNumberToAddress(start_page_for_original_block));
            for (auto it = copy_map.begin(); it !=copy_map.end(); ++it) {
                func(OpCode::READ, translatePageNumberToAddress(it->second));
                Address original = translatePageNumberToAddress(corresponding_map.find(it->first)->second);
                func(OpCode::WRITE, original);
            }
            func(OpCode::ERASE, overprovision_page_address);
        } else {
            ans = true;
            for (auto it = copy_map.begin(); it !=copy_map.end(); ++it) {
                func(OpCode::READ, translatePageNumberToAddress(it->second));
                func(OpCode::WRITE, translatePageNumberToAddress(it->first));
            }
            func(OpCode::ERASE, overprovision_page_address);
            func(OpCode::ERASE, translatePageNumberToAddress(start_page_for_original_block));
            for (auto it = corresponding_map.begin(); it != corresponding_map.end(); ++it) {
                func(OpCode::READ, translatePageNumberToAddress(it->first));
                func(OpCode::WRITE, translatePageNumberToAddress(it->second));
            }
            func(OpCode::ERASE, translatePageNumberToAddress(cleaning_reservation_page_index));
        }
        updateEraseEecordMap(start_page_for_original_block);
        updateEraseEecordMap(start_page_for_overprovision_block);
        if (ans) updateEraseEecordMap(cleaning_reservation_page_index);
        return ans;
    }

    /* 
     * Determine not reach erase limit, return true if not.
     */
    bool notReachEraseLimit(size_t page_index ) {
        size_t block_index = getBlockIndex(page_index);
        if (erase_record_map[block_index] >= block_erase_count) {
            return false;
        }
        return true;
    }


    /* 
     * Update EraseEecordMap
     */
    void updateEraseEecordMap(size_t page_index) {
        size_t block_index = getBlockIndex(page_index);
        size_t times = erase_record_map[block_index];
        erase_record_map[block_index] = times + 1;
    }

    /* 
     * Get block index
     */
    size_t getBlockIndex(size_t page_index) {
        return page_index / block_size;
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
     * Compute page index given block index and lba.
     */
    size_t getPageIndexInCertainBlock(size_t block_index, size_t lba) {
        return block_index * block_size + lba % block_size;
    }

    /* 
     * Compute corrspoinding block index based on address
     */
    size_t translateAddressToBlockIndex(Address address) {
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
