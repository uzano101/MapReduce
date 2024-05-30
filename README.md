Sure, here's a detailed and visually appealing README for the project:

---

# MapReduce - Multi Threaded Programming 



## Overview

Welcome to the **MapReduce - Multi Threaded Programming** project! This implementation leverages the power of multi-threading to efficiently process large datasets using the MapReduce paradigm. This project demonstrates a robust and scalable approach to parallel data processing using C++ and POSIX threads (pthreads).

## Features

- **Multi-threaded Execution:** Leverage multiple CPU cores for parallel data processing.
- **Custom Map and Reduce Functions:** Easily define your own map and reduce operations.
- **Progress Monitoring:** Track the job's progress through different stages.
- **Thread Synchronization:** Efficiently manage thread synchronization using barriers and mutexes.

## Architecture 

The project is structured around two main components:
1. **MapReduceClient:** An interface for user-defined map and reduce functions.
2. **MapReduceFramework:** The core framework that manages job execution, threading, and synchronization.



## Getting Started

### Prerequisites

- C++ Compiler (g++ recommended)
- POSIX Threads Library
- CMake (optional, for building the project)

### Building the Project

To build the project, you can use the provided Makefile or CMake. Here’s how to do it using CMake:

```sh
mkdir build
cd build
cmake ..
make
```

### Usage

1. **Define Your MapReduce Client:**

   Implement the `MapReduceClient` interface with your custom map and reduce logic.

   ```cpp
   class MyMapReduceClient : public MapReduceClient {
   public:
       void map(const K1* key, const V1* value, void* context) const override {
           // Your map implementation
       }

       void reduce(const IntermediateVec* pairs, void* context) const override {
           // Your reduce implementation
       }
   };
   ```

2. **Prepare Input Data:**

   Populate the input vector with your data.

   ```cpp
   InputVec inputVec = {/* Populate input vector */};
   OutputVec outputVec;
   ```

3. **Start the MapReduce Job:**

   ```cpp
   MyMapReduceClient client;
   JobHandle jobHandle = startMapReduceJob(client, inputVec, outputVec, 4);
   ```

4. **Monitor Job Progress:**

   ```cpp
   JobState state;
   while (true) {
       getJobState(jobHandle, &state);
       std::cout << "Stage: " << state.stage << ", Progress: " << state.percentage << "%" << std::endl;
       if (state.stage == REDUCE_STAGE && state.percentage == 100) {
           break;
       }
       usleep(100000); // Sleep for a short duration
   }
   ```

5. **Wait for Job Completion:**

   ```cpp
   waitForJob(jobHandle);
   ```

6. **Close the Job Handle:**

   ```cpp
   closeJobHandle(jobHandle);
   ```

7. **Process the Output:**

   ```cpp
   for (const auto& pair : outputVec) {
       // Handle output
   }
   ```

### Example

Here’s a complete example to demonstrate the usage:

```cpp
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include <iostream>

class MyMapReduceClient : public MapReduceClient {
public:
    void map(const K1* key, const V1* value, void* context) const override {
        // Example map implementation
    }

    void reduce(const IntermediateVec* pairs, void* context) const override {
        // Example reduce implementation
    }
};

int main() {
    MyMapReduceClient client;
    InputVec inputVec = {/* Populate input vector */};
    OutputVec outputVec;

    JobHandle jobHandle = startMapReduceJob(client, inputVec, outputVec, 4);

    JobState state;
    while (true) {
        getJobState(jobHandle, &state);
        std::cout << "Stage: " << state.stage << ", Progress: " << state.percentage << "%" << std::endl;

        if (state.stage == REDUCE_STAGE && state.percentage == 100) {
            break;
        }

        usleep(100000); // Sleep for a short duration
    }

    waitForJob(jobHandle);
    closeJobHandle(jobHandle);

    for (const auto& pair : outputVec) {
        // Handle output
    }

    return 0;
}
```

## Contribution

Contributions are welcome! If you have suggestions for improvements or new features, feel free to create a pull request or open an issue.

---

### Authors

- **Your Name** - [GitHub]([https://github.com/your-github-profile](https://github.com/uzano101))
- **Contributors** - Yael Yanuka

---

Thank you for visiting the **MapReduce - Multi Threaded Programming** project. We hope you find it useful and educational. Happy coding!


---
