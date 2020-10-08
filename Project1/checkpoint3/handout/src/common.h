/**
 * @file common.h
 * @brief This file is a common header between parent and child process
 *
 * This file essentially have various bases classes from which both
 * child and parent derive classes. These classes are then meant to interact
 * with each other with IPC
 *
 * @author Saksham Jain (sakshamj)
 * @bug No known bug
 */

#ifndef __COMMON_H__
#define __COMMON_H__

/* TODO: Remove unneccesary headers */

#include <dlfcn.h>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <unordered_set>
#include <queue>

#include <stdint.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "config.h"


/* Takes maximum */
#define MAX(a, b)	((a) > (b) ? (a) : (b))

/* Pipes recevie and transmit end - As per <unistd.h> */
#define PIPE_RX_END	0
#define PIPE_TX_END	1

/* Parent passed pipe fd in argv to child. These define the offset in argv */
#define CHILD_PIPE_RX_FD_ARGV_OFF 1 /* 0th is reserved for child's exe name */
#define CHILD_PIPE_TX_FD_ARGV_OFF 2

/* Max length of string when pipefd when converted to string */
#define MAX_PIPEFD_STR_LEN 10

#define PAGE_SIZE	4096


/*
 * Various strings used in configuration file
 * Any change in these defines need corresponding changes in the
 * configuration file
 */

#define CONF_S_SSD_SIZE		"SSD_SIZE"
#define CONF_S_PACKAGE_SIZE	"PACKAGE_SIZE"
#define CONF_S_DIE_SIZE		"DIE_SIZE"
#define CONF_S_PLANE_SIZE	"PLANE_SIZE"
#define CONF_S_BLOCK_SIZE	"BLOCK_SIZE"
#define CONF_S_BLOCK_ERASES	"BLOCK_ERASES"
#define CONF_S_OVERPROVISIONING	"OVERPROVISIONING"
#define CONF_S_GCPOLICY		"SELECTED_GC_POLICY"

// Configs for checkpoint 3 grading.
#define CONF_S_MEMORY_BASELINE "MEMORY_BASELINE"
#define CONF_S_WRITES_BASELINE "WRITES_BASELINE"
#define CONF_S_WRITE_AMPLIFICATION_THRESHOLD "WRITE_AMPLIFICATION_THRESHOLD"
#define CONF_S_WRITES_THRESHOLD "WRITES_THRESHOLD"

#define CONF_S_WEIGHT_WRITE_AMPLIFICATION_INFINITE \
    "WEIGHT_WRITE_AMPLIFICATION_INFINITE"
#define CONF_S_WEIGHT_MEMORY_INFINITE "WEIGHT_MEMORY_INFINITE"
#define CONF_S_WEIGHT_ENDURANCE_INFINITE "WEIGHT_ENDURANCE_INFINITE"
#define CONF_S_WEIGHT_WRITE_AMPLIFICATION_FINITE \
    "WEIGHT_WRITE_AMPLIFICATION_FINITE"
#define CONF_S_WEIGHT_MEMORY_FINITE "WEIGHT_MEMORY_FINITE"


/* Common global data (Between FTL and FlashSim - Not shared, each has copy) */
struct Common_t {

	/* Forked child's pid */
	pid_t child_pid;

	/* Pipes - For IPC between child and process */
	int pipefd[2];
};

/* Common global data */
extern struct Common_t Common;

/*
 * PageType determines the virtual size allocated to a page
 * Whole logical page size is not used to store a page
 */
#if ENABLE_LARGE_DATASTORE_PAGE
class datastore_page_t {

public:
	datastore_page_t() {};

	char buf[PAGE_SIZE];
};
#define TEST_PAGE_TYPE class datastore_page_t

#else
#define TEST_PAGE_TYPE uint32_t
#endif

/*
 * class ConfBase - Base class for getting configuration of flash
 *
 * Contain virtual functions which should be overriden in the derived classes
 */

class ConfBase {

	public:

	ConfBase() {}

	virtual ~ConfBase() {}

	/* All these functions need to be overriden */

	/* Returns the number of packages in flash */
	virtual size_t GetSSDSize(void) const {
		assert(0);
		return size_t(-1);
	}

	/* Returns the number of dies in flash */
	virtual size_t GetPackageSize(void) const {
		assert(0);
		return size_t(-1);
	}

	/* Returns the number of planes in flash */
	virtual size_t GetDieSize(void) const {
		assert(0);
		return size_t(-1);
	}

	/* Returns the number of blocks in flash */
	virtual size_t GetPlaneSize(void) const {
		assert(0);
		return size_t(-1);
	}

	/* Returns the number of pages in flash */
	virtual size_t GetBlockSize(void) const {
		return size_t(-1);
	}

	/* Returns the block lifetime of flash */
	virtual size_t GetBlockEraseCount(void) const {
		assert(0);
		return size_t(-1);
	}

	/* Returns the overprovisioning (as percentage) of flash */
	virtual size_t GetOverprovisioning(void) const {
		assert(0);
		return size_t(-1);
	}

	/* Returns the garbage collection policy of flash */
	virtual size_t GetGCPolicy(void) const {
		assert(0);
		return size_t(-1);
	}

	/*
	 * Returns the string corresponding to string (as in conf file)
	 * It is preferred not to call this function directly
	 * but to use other functions
	 */
	virtual const std::string &GetString(const std::string &key) const  {
		assert(0);
		return key;
	}

	/*
	 * Returns the Integer corresponding to string (as in conf file)
	 * It is preferred not to call this function directly
	 * but to use other functions
	 */
	virtual int GetInteger(const std::string &key) const {
		(void)key;
		assert(0);
		return int(-1);
	}

	/*
	 * Returns the Double corresponding to string (as in conf file)
	 * It is preferred not to call this function directly
	 * but to use other functions
	 */
	virtual double GetDouble(const std::string &key) const {
		(void)key;
		assert(0);
		return double(-1);
	}

};

/*
 * class Address - The POD type for physical addresses on each level
 *                 of SSD
 *
 * According to our assumed architecture of an SSD, the address components
 * are: package, die, plane, block, page, from top level to bottom level.
 * Pages are atomic unit for read and write, and thus could not be further
 * divided into smaller parts. All read/write operations must use full
 * addresses as the input to the controller.
 */
class Address {

	public:
	/*
	 * There are no special reason for choosing uint8_t for the first
	 * and uint16_t for the second - just to make the structure as
	 * compact as possible
	 *
	 * The entire structure could be stored inside a 64 bit register which
	 * makes argument passing & STL operations very fast (though it should)
	 * not be an issue in most cases
	 */

	uint8_t package;
	uint8_t die;
	uint16_t plane;
	uint16_t block;
	uint16_t page;

	/*
	 * Constructor - Initialize the object to an unknown state
	 */
	Address() {}

	/*
	 * Constructor - Initialize the object to a known state by providing
	 *               all components of an address
	 */
	Address(uint8_t p_package,
    		uint8_t p_die,
    		uint16_t p_plane,
    		uint16_t p_block,
		uint16_t p_page) :
		package{p_package},
		die{p_die},
		plane{p_plane},
		block{p_block},
		page{p_page}
		{}

	/*
	 * Constructor - Initialize all components except page
	 *
	 * This is majorly used for block level address construction
	 */

	Address(uint8_t p_package,
		uint8_t p_die,
		uint16_t p_plane,
		uint16_t p_block) :
		package{p_package},
		die{p_die},
		plane{p_plane},
		block{p_block},
		page{0}
		{}

	/*
	 * Copy Constructor - For copy construction
	 */
	Address(const Address &addr) :
		package{addr.package},
		die{addr.die},
		plane{addr.plane},
		block{addr.block},
		page{addr.page}
		{}

	/*
	 * Prints the address - Useful for debugging
	 * fp - File pointer
	 */
	void Print(FILE *fp)
	{
		fprintf(fp, "Address is Package: %d, Die %d,"
				"Plane: %d, Block: %d, Page: %d\n",
				package, die, plane, block, page);
	}
};

/*
 * enum class OpCode - This is the opcode issued from FTL to controller
 *                     for read amplification and write amplification
 */
enum class OpCode {

	/* Read a page into the buffer */
	READ = 0,

	/* Write a page into the buffer */
	WRITE,

	/* Erase a block (page ID is ignored) */
	ERASE,
};

/*
 * enum class ExecState - State of execution returned from the FTL
 */
enum class ExecState {
	SUCCESS = 0,
	FAILURE,
};

/*
 * class ExecCallBack() - Proxy class for controller to let FTL call
 *                        	  its function without exposing controller
 *                        	  internals to the FTL
 *
 * This is a base class. Derive classes from it.
 */

template <typename PageType>
class ExecCallBack {

	public:

	/*
	 * Constructor
	 */
	ExecCallBack() {
	}

	/*
	 * Destructor
	 */
	virtual ~ExecCallBack() {
	}

	/*
	 * operator() - Mimics the behavior of a function call that calls
	 *              ExecuteCommand() of class Controller
	 */
    	virtual void operator()(OpCode operation, Address addr) const {

		(void)operation;
		(void)addr;
		assert(0);
	}
};
/*
 * class FTLBase - The base class for FTL
 *
 * This class is a pure virtual one, and could not be instantiated.
 * If you are implementing the actual FTL functionality, please define
 * a subclass of this class, and implement/add your own methods
 *
 * Two basic interfaces for FTL are ReadTranslate() and WriteTranslate(),
 * which are called for read and write operations. Both member functions
 * accept the LBA (linear block address, which is the linear address of
 * a page - do not be confused by the obsolete terminology "block address")
 * and a vector pointer as arguments. The LBA is the input argument, and
 * the vector is an output argument.
 *
 * The element of the output vector argument is std::pair<OpCode, Address>.
 * This vector represents the sequence of operations that should be performed
 * by the controller in order to conduct the operation requested (read/write).
 * OpCode is a enum class defined as the operation an FTL should perform,
 * which are READ, WRITE and ERASE (we only need these three, though in
 * a real SSD the instructions are far more richer than our simple model).
 * Address is class Address object, which points to the page or block the
 * operation should be performed on.
 *
 * If the operation is READ or WRITE, then all five fields in Address will
 * be used. Otherwise for MERGE, Only the first four fields in Address will
 * be used, and "page" field will be ignored.
 */

template <typename PageType>
class FTLBase {

	/*
	 * Make all interface classes public so that class Controller
    	 * has access to them
	 */

	public:

	FTLBase() {};

    	/*
	 * The destructor must be made virtual to make deleting the object
	 * through base class pointer possible without any memory leak
	 */
    	virtual ~FTLBase() {};

	virtual std::pair<ExecState, Address>
	ReadTranslate(size_t, const ExecCallBack<PageType> &) {

		assert(0);
		return std::make_pair(ExecState::FAILURE, Address());

	}

    	virtual std::pair<ExecState, Address>
    	WriteTranslate(size_t, const ExecCallBack<PageType> &) {

		assert(0);
		return std::make_pair(ExecState::FAILURE, Address());

	}

    	virtual ExecState
    	Trim(size_t, const ExecCallBack<PageType> &) {

		assert(0);
		return ExecState::FAILURE;

	}

	/*
	 * Students don't need to implement this
	 * Will never be called in student's version
	 */
	virtual size_t GetFTLStackSize(void) {

		assert(0);
		return 0;
	};

};


/* Enum to specify the type of message in IPC and owner (child and parent) */
enum message_owner_t {
	OWNER_FTL = 0,
	OWNER_FLASHSIM,
};

enum message_type_t {

	/* Placing value to all enum helps in debugging */

	/* Empty message */
	MSG_EMPTY = 0,

	/* Child collects various configuration results */
	MSG_CONF_REQ_SSD_SIZE = 1,
	MSG_CONF_REQ_PACKAGE_SIZE = 2,
	MSG_CONF_REQ_DIE_SIZE = 3,
	MSG_CONF_REQ_PLANE_SIZE = 4,
	MSG_CONF_REQ_BLOCK_SIZE = 5,
	MSG_CONF_REQ_BLOCK_ERASES = 6,
	MSG_CONF_REQ_OVERPROVISIONING = 7,
	MSG_CONF_REQ_GCPOLICY = 8,

	/* Parent responds to various configuration queries */
	MSG_CONF_RES_SSD_SIZE = 9,
	MSG_CONF_RES_PACKAGE_SIZE = 10,
	MSG_CONF_RES_DIE_SIZE = 11,
	MSG_CONF_RES_PLANE_SIZE = 12,
	MSG_CONF_RES_BLOCK_SIZE = 13,
	MSG_CONF_RES_BLOCK_ERASES = 14,
	MSG_CONF_RES_OVERPROVISIONING = 15,
	MSG_CONF_RES_GCPOLICY = 16,

	/* Parent sends instructions to child (myFTL)*/
	MSG_FTL_INSTR_READ = 17,
	MSG_FTL_INSTR_WRITE = 18,
	MSG_FTL_INSTR_TRIM = 19,

	/* Parent receives instruction's responses (from myFTL)*/
	MSG_FTL_READ_RESP = 20,
	MSG_FTL_WRITE_RESP = 21,
	MSG_FTL_TRIM_RESP = 22,

	/* Child request parent's services (Flashsim) */
	MSG_SIM_REQ_READ = 23,
	MSG_SIM_REQ_WRITE = 24,
	MSG_SIM_REQ_ERASE = 25,

	/* Used by child to tell parent it has woken up */
	MSG_FTL_WAKEUP = 26,

	/* Used to gather information from child about stack */
	MSG_FTL_STACK_SIZE_REQ = 27,
	MSG_FTL_STACK_SIZE_RESP = 28,
};

/* Structure to specify format of communication between parent and child */
class IPC_Format {

	public:

	/*
	 * Owner of the message - Not essential, but used for assertions
	 * Inidicated the message originator
	 */
	enum message_owner_t owner;
	/* Indicates the type of response */
	enum message_type_t type;

	/* The actual data - Which members are filled depends on msg type*/
	/* TODO: Can make this union to save space */

	/*
	 * Configuration request's response
	 * E.g.  size_t GetSSDSize(void)
	 */
	size_t conf_resp;

	/*
	 * Used for sending requests to ftl
	 * E.g. WriteTranslate(size_t lba,
	 * 			const ExecCallBack<PageType> &)
	 */
	size_t lba;


	/* Stack size of child */
	size_t child_stack_size;

	/*
	 * Response fields from FTL
	 * E.g.
	 * std::pair<ExecState, Address>
	 * ReadTranslate(size_t, const FlashSimExecCallBack<PageType> &)
	 */
	ExecState ftl_resp_execstate;
	Address ftl_resp_addr;

	/* Opcode used for sending request to flashsim */
	OpCode sim_req_opcode;

	/* Address sent to flashsim along with request */
	Address sim_req_addr;

	IPC_Format() {
	}

	~IPC_Format() {
	}

};


#endif /* __COMMON_H__ */
