# Design and Dependency Injection

**Author:** Gisela Belmonte Cruz  
**Repository:** [https://github.com/giselabcruz/tscd-datalake-adapter](https://github.com/giselabcruz/tscd-datalake-adapter)  
**Date:** November 2025

---

## 4. Reflection

### 1. What specific dependencies prevented the code from being portable?

In the original version, the project directly depended on the **AWS SDK for Java**, meaning that all storage logic was tightly coupled to the **S3 API** — including client creation, credentials, and endpoints.  
As a result, switching to another provider (such as Azure Blob Storage or a local file system) would have required modifying large parts of the code.

After the redesign, this dependency was isolated into a dedicated adapter (`S3StorageAdapter`), while the application interacts only through a common interface (`ObjectStoragePort`).  
This effectively removes vendor lock-in and allows the system to change the storage backend without altering business logic.

---

### 2. What impact does this redesign have on the project's maintainability?

The redesign has a **highly positive impact**.  
The system is now divided into **clearly defined layers**: application, domain, and infrastructure.  
For example, the `IngestionService` class no longer needs to know how a file is stored — it simply delegates the operation to a “datalake” that follows a defined contract.

This separation makes it easy to add a new storage provider or change configurations by only creating a new adapter or updating environment variables.  
Overall, the project becomes cleaner, easier to understand, and much more maintainable in the long run.

---

### 3. How does dependency injection facilitate unit testing?

Dependency injection makes it possible to replace real components with **mock implementations** during testing.

In this project, the ingestion service can be tested using a `MockStorageAdapter` that stores data in memory instead of S3.  
This eliminates the need for external services during tests, speeds up the process, and allows behavior to be validated in complete isolation.

In summary, dependency injection improves test **speed, control, and reliability**.

---

### 4. Which object-oriented design principles are most useful to avoid vendor lock-in?

The following design principles have been the most relevant in this refactor:

- **DIP (Dependency Inversion Principle):**  
  High-level modules depend on abstractions rather than concrete implementations.  
  The application no longer depends on AWS S3 directly, but on the `ObjectStoragePort` interface.

- **OCP (Open/Closed Principle):**  
  The system is open for extension (e.g., adding Azure or LocalFS adapters) but closed for modification.  
  New providers can be integrated without changing existing code.

- **SoC (Separation of Concerns):**  
  Business logic, infrastructure, and configuration are separated, avoiding cross-responsibilities and improving readability and maintenance.

Thanks to these principles, the system is no longer tied to a specific vendor.  
By using interfaces, dependency injection, and concrete adapters, the project has evolved into a **more professional, flexible, and testable** architecture.

