#include "common.h"
#include "myFTL.h"
#include "unordered_map"
#include "math.h"
#include <time.h>

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
bool debug_print;

size_t overall_block_capacity;
size_t overall_pages_capacity;
size_t available_block_number;
size_t available_pages_number;
size_t overprovision_block_number;
size_t overprovision_pages_number;

size_t cleaning_reservation_block_number;
size_t log_reservation_block_number;
size_t upper_threshold_for_log_reservation_page_number;

// // LBA block index to PBA block index
// std::unordered_map<size_t, size_t> lba_block_index_to_pba_block_index_map;

// // std::vector<size_t> lba_block_index_to_pba_block_index_map

// // PBA data block index to log reservation block page index 
// std::unordered_map<size_t, size_t> pba_data_block_index_to_log_reservation_block_index_map;

// /* block index in available blocks as key, page index in overprovision blocks as value */
// std::unordered_map<size_t, size_t> pba_page_index_map;

// /* To record erase time for this block, key is block index, to determine wear out */
// std::unordered_map<size_t, size_t> erase_record_map;

// std::unordered_map<size_t, size_t> log_reservation_to_available_page_map;

/* key block index, value timestamp, every write time plus one */
// std::unordered_map<size_t, size_t> lru_record_map;
// size_t physical_page_index;
size_t write_index;
size_t log_reservation_page_index;
size_t cleaning_reservation_page_index;
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
    gc_policy = 0;
    // gc_policy = conf->GetGCPolicy();
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

    size_t lba_block_index_to_pba_block_index_map[available_block_number];
    memset(lba_block_index_to_pba_block_index_map, -1, sizeof(size_t) * overall_block_capacity);
    // PBA data block index to log reservation block page index 
    size_t pba_data_block_index_to_log_reservation_block_index_map[available_block_number];
    memset(pba_data_block_index_to_log_reservation_block_index_map, -1, sizeof(size_t) * available_block_number);

    /* block index in available blocks as key, page index in overprovision blocks as value */
    size_t pba_page_index_map[available_pages_number];
    memset(erase_record_map, -1, sizeof(size_t) * overall_block_capacity

    /* To record erase time for this block, key is block index, to determine wear out */
    size_t erase_record_map[overall_block_capacity];
    memset(erase_record_map, 0, sizeof(size_t) * overall_block_capacity

    size_t log_reservation_to_available_page_map[overprovision_block_number];
    memset(erase_record_map, -1, sizeof(size_t) * overall_block_capacity


    printf("SSD Configuration: %zu, %zu, %zu, %zu, %zu\n",
		ssd_size, package_size, die_size, plane_size, block_size);
	printf("Max Erase Count: %zu, Overprovisioning: %zu\n", 
		block_erase_count, op);
    printf("Garbage Collection Policy %zu\n", gc_policy);
    printf("available_block_number %zu, overprovision_block_number %zu, log_reservation_block_number %zu, cleaning_reservation_block_number %zu\n", available_block_number, 
    overprovision_block_number, log_reservation_block_number, cleaning_reservation_block_number);
    printf("cleaning_reservation_page_index %zu, log_reservation_page_index %zu, overall_pages_capacity %zu\n", cleaning_reservation_page_index, log_reservation_page_index, overall_pages_capacity);
    printf("cleaning_reservation_block_index %zu, log_reservation_block__index %zu, overall_pages_block_index %zu\n", getBlockIndex(cleaning_reservation_page_index), getBlockIndex(log_reservation_page_index), getBlockIndex(overall_pages_capacity));
    write_index = 0;
    debug_print = true;
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
        if (lba_block_index_to_pba_block_index_map.find(lba_block_index) == lba_block_index_to_pba_block_index_map.end()) {
            return std::make_pair(ExecState::FAILURE, Address(0, 0, 0, 0, 0));
        }
        size_t pba_block_index = lba_block_index_to_pba_block_index_map.find(lba_block_index)->second;
        size_t pba_page_index = getPageIndexInCertainBlock(pba_block_index, lba);
        if (pba_page_index_map.find(pba_page_index) == pba_page_index_map.end()) {
            return std::make_pair(ExecState::FAILURE, Address(0, 0, 0, 0, 0));
        } else {
            size_t actual_page_index = pba_page_index_map.find(pba_page_index)->second;
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
        write_index++;
        if (debug_print) printf("Write index %zu, lba %zu\n", write_index, lba);
        if (lba >= available_pages_number) {
             /* check if lba is valid */
            if (debug_print) {
                printf("1\n");
            }
            return std::make_pair(ExecState::FAILURE, Address(0, 0, 0, 0, 0));
        }
        size_t lba_block_index = getBlockIndex(lba);
        if (lba_block_index_to_pba_block_index_map.find(lba_block_index) == lba_block_index_to_pba_block_index_map.end()) {
            size_t pba_block_index = lba_block_index;
            lba_block_index_to_pba_block_index_map[lba_block_index] = pba_block_index;
            Address new_address = translatePageNumberToAddress(lba);
            pba_page_index_map[lba] = lba;
            return std::make_pair(ExecState::SUCCESS, new_address);
        } else {
            size_t pba_block_index = lba_block_index_to_pba_block_index_map.find(lba_block_index)->second;
            size_t pba_page_index = getPageIndexInCertainBlock(pba_block_index, lba);
            if (pba_page_index_map.find(pba_page_index) == pba_page_index_map.end()) {
                Address new_address = translatePageNumberToAddress(pba_page_index);
                pba_page_index_map[pba_page_index] = pba_page_index;
                return std::make_pair(ExecState::SUCCESS, new_address);
            } else {
                if (pba_data_block_index_to_log_reservation_block_index_map.find(pba_block_index) == pba_data_block_index_to_log_reservation_block_index_map.end()) {
                    if (log_reservation_page_index >= upper_threshold_for_log_reservation_page_number || 
                        log_reservation_to_available_page_map.find(log_reservation_page_index) != log_reservation_to_available_page_map.end()) {
                        if (!roundRobinGarbageCollection(func)) {
                            return std::make_pair(ExecState::FAILURE, Address(0, 0, 0, 0, 0));
                        }
                        return WriteTranslate(lba, func);
                    }
                    if (debug_print) {
                        printf("8\n");
                    }
                    /* Assign new overprovision block */
                    Address new_block_addresss = translatePageNumberToAddress(log_reservation_page_index);
                    log_reservation_to_available_page_map[log_reservation_page_index] = pba_page_index;
                    pba_page_index_map[pba_page_index] = log_reservation_page_index;
                    pba_data_block_index_to_log_reservation_block_index_map[pba_block_index] = log_reservation_page_index + 1;
                    log_reservation_page_index += block_size;
                    if (debug_print) {
                        printf("Debug in first write to new overprovision block\n");
                        debug();
                    }
                    return std::make_pair(ExecState::SUCCESS, new_block_addresss);
                } else {
                    if (debug_print) printf("4\n");
                    /* This block has corresponding overprovision block*/
                    size_t overprovision_page_index = pba_data_block_index_to_log_reservation_block_index_map.find(pba_block_index)->second;
                    Address overprovision_page_address = translatePageNumberToAddress(overprovision_page_index);
                    size_t block_index_in_overprovision = translateAddressToBlockIndex(overprovision_page_address);
                    if (overprovision_page_index == overall_pages_capacity) {
                        if (debug_print)  {
                            printf("4\n");
                            printf("need to clean block\n");
                        }
                        /* corresponding overprovision block is full*/
                        if(!cleaningForFullLogReservationBlock(pba_block_index, overprovision_page_index - block_size, func)) return std::make_pair(ExecState::FAILURE, Address(0, 0, 0, 0, 0));
                        if (debug_print)  {
                            printf("Debug after erase in corresponding block\n");
                            debug();
                        }
                        return WriteTranslate(lba, func);
                    } else {
                        if (debug_print) printf("6\n");
                        pba_page_index_map[pba_page_index] = overprovision_page_index;
                        if (debug_print) {
                            printf("Debug in writing to unfull block in overprovision\n");
                            printf("overprovision_page_index %zu\n", overprovision_page_index);
                            debug();
                        }
                        if (overprovision_page_address.page == block_size - 1) {
                            pba_data_block_index_to_log_reservation_block_index_map[pba_block_index] = overall_pages_capacity;
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
        if (debug_print) {
            printf("Enter roundRobinGarbageCollection\n");
        }
        size_t start_page_for_overprovision_block = garbage_collection_log_reservation_page_index;
        size_t page_in_available_block = log_reservation_to_available_page_map.find(start_page_for_overprovision_block)->second;
        size_t old_block_index = getBlockIndex(page_in_available_block);
        size_t start_page_for_original_block = old_block_index * block_size;
        size_t block_index_in_overprovision = getBlockIndex(start_page_for_overprovision_block);
        // printf("start_page_for_original_block %zu, block_index_in_overprovision %zu, start_page_for_overprovision_block %zu\n", start_page_for_original_block, 
        //  block_index_in_overprovision, start_page_for_overprovision_block);
         if (debug_print) {
            printf("roundRobinGarbageCollection 2\n");
            printf("erase_record_map\n");
            for (auto it = erase_record_map.begin(); it != erase_record_map.end(); ++it) {
                printf("block index %zu, times %zu \n", it->first, it->second);
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
        log_reservation_page_index = garbage_collection_log_reservation_page_index;
        garbage_collection_log_reservation_page_index += block_size;
        if (garbage_collection_log_reservation_page_index >= upper_threshold_for_log_reservation_page_number) {
            garbage_collection_log_reservation_page_index = available_pages_number;
        }
        pba_data_block_index_to_log_reservation_block_index_map.erase(old_block_index);
        log_reservation_to_available_page_map.erase(start_page_for_overprovision_block);
        return true;
    }

    /*
     * Used by those garbage collection policy
     */
    bool cleaningForFullLogReservationBlock(size_t pba_data_block_index, size_t over_provision_log_page_index, const ExecCallBack<PageType> &func) {
        if (debug_print) {
            printf("Enter Erase\n");
        }
        size_t start_page_for_original_block = pba_data_block_index * block_size;
        size_t block_index_in_overprovision = getBlockIndex(over_provision_log_page_index);
        size_t start_page_for_overprovision_block = block_index_in_overprovision * block_size;
        if (debug_print) {
            printf("start_page_for_original_block %zu, block_index_in_overprovision %zu, start_page_for_overprovision_block %zu, cleaning_reservation_page_index %zu\n", 
                start_page_for_original_block, block_index_in_overprovision, start_page_for_overprovision_block, cleaning_reservation_page_index);
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
        return true;
    }

    bool performErase(size_t cleaning_reservation_page_index, size_t start_page_for_original_block, size_t start_page_for_overprovision_block, const ExecCallBack<PageType> &func) {
        printf("performErase\n");
        size_t curr_cleaning_page_index = cleaning_reservation_page_index;
        Address overprovision_page_address = translatePageNumberToAddress(start_page_for_overprovision_block);
        std::unordered_map<size_t, size_t> copy_map;
        std::unordered_map<size_t, size_t> corresponding_map;
        bool ans = false;
        if (debug_print) {
            printf("start_page_for_original_block %zu,  start_page_for_overprovision_block %zu, cleaning_reservation_page_index %zu\n", start_page_for_original_block,  start_page_for_overprovision_block, 
            cleaning_reservation_page_index);
            printf("original_block_index %zu,  overprovision_block_index %zu, cleaning_reservation_block_index%zu\n", getBlockIndex(start_page_for_original_block),  getBlockIndex(start_page_for_overprovision_block), 
            getBlockIndex(cleaning_reservation_page_index));
        }
        for (size_t i = start_page_for_original_block; i < start_page_for_original_block + block_size; i++) {
            // printf("performe i %zu\n", i);
            if (pba_page_index_map.find(i) != pba_page_index_map.end()) {
                copy_map[curr_cleaning_page_index] = pba_page_index_map.find(i)->second;
                corresponding_map[curr_cleaning_page_index] = i;
                pba_page_index_map[i] = i;
                curr_cleaning_page_index++;
            }
        }
        if (copy_map.size() == 1) {
            printf("performe erase without using cleaning block \n");
            func(OpCode::ERASE, translatePageNumberToAddress(start_page_for_original_block));
            for (auto it = copy_map.begin(); it !=copy_map.end(); ++it) {
                func(OpCode::READ, translatePageNumberToAddress(it->second));
                Address original = translatePageNumberToAddress(corresponding_map.find(it->first)->second);
                func(OpCode::WRITE, original);
            }
            func(OpCode::ERASE, overprovision_page_address);
        } else {
            printf("performe erase using cleaning block \n");
            ans = true;
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
            func(OpCode::ERASE, translatePageNumberToAddress(cleaning_reservation_page_index));
        }
        updateEraseEecordMap(start_page_for_original_block);
        updateEraseEecordMap(start_page_for_overprovision_block);
        if (ans) updateEraseEecordMap(cleaning_reservation_page_index);
        if (debug_print) debug();
        return ans;
    }

    /* 
     * Determine not reach erase limit, return true if not.
     */
    bool notReachEraseLimit(size_t page_index ) {
        size_t block_index = getBlockIndex(page_index);
        if (erase_record_map.find(block_index) != erase_record_map.end() && erase_record_map.find(block_index)->second >= block_erase_count) {
            return false;
        }
        return true;
    }


    /* 
     * Update EraseEecordMap
     */
    void updateEraseEecordMap(size_t page_index) {
        size_t block_index = getBlockIndex(page_index);
        if (erase_record_map.find(block_index) == erase_record_map.end()) {
            erase_record_map[block_index] = 1;
        } else {
            size_t times = erase_record_map.find(block_index)->second;
             erase_record_map[block_index] = times + 1;
        }
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

    bool withInDataBlock(size_t pageIndex) {
        return pageIndex < available_pages_number;
    }

    bool inDataBlock(size_t pba_page_index) {
        return pba_page_index < available_pages_number;
    }

    /*
     * Optionally mark a LBA as a garbage.
     */
    ExecState
    Trim(size_t lba, const ExecCallBack<PageType>& func) {
        (void) lba;
        (void) func;
        printf("Trim called\n");
        return ExecState::SUCCESS;
    }

    void debug() {
        // printf("Debug first_write_lba_pba_map\n");
        // for (auto it = first_write_lba_pba_map.begin(); it !=first_write_lba_pba_map.end(); ++it) {
        //     printf("lba %zu, pga %zu \n",it->first, it->second);
        // }
        // printf("************************************\n");
        // printf("unprovision_lba_pba_map\n");
        // for (auto it = unprovision_lba_pba_map.begin(); it != unprovision_lba_pba_map.end(); ++it) {
        //     printf("lba %zu, pga %zu \n", it->first, it->second);
        // }
        // printf("************************************\n");
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
