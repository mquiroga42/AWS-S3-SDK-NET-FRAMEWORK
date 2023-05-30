using Amazon.S3;
using Amazon.S3.Model;
using System.Net;
using Amazon.Runtime;
using Amazon;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using System.Threading;
using System;
using System.Collections.Generic;
using Amazon.S3.Util;
using Serilog;
using Amazon.Runtime.CredentialManagement;

namespace SDK_NET_AWS_S3
{
	/// <summary>
	/// Independent module that provides a multi-regional service associated with a client.
	/// <br>If another client is required, another instance of this class can be created and stored in a List ModuleAWSBOXS3.</br>
	/// </summary>
	public class ModuleAWSBOXS3
	{
		/// <summary>
		/// Local logging configuration.
		/// </summary>
		private ILogger _log = new LoggerConfiguration()
			.MinimumLevel.Debug()
			.WriteTo.Console()
			.CreateLogger();

		/// <summary>
		/// Stores a list of clients and manages regions.
		/// </summary>
		private readonly List<AmazonS3Client> s3Clients = new List<AmazonS3Client>();

		/// <summary>
		/// Stores the active session in the SDK.
		/// </summary>
		private AmazonS3Client s3Client;

		/// <summary>
		/// Gets the Amazon S3 client associated with the active session.
		/// </summary>
		public AmazonS3Client S3Client { get { return s3Client; } }

		/// <summary>
		/// Represents the selected bucket.
		/// </summary>
		private string activeBucketName = string.Empty;

		/// <summary>
		/// Gets the name of the active bucket.
		/// </summary>
		public string ActiveBucketName { get { return activeBucketName; } }

		/// <summary>
		/// Represents the active configuration profile.
		/// </summary>
		private string activeProfile = "Default";


		// ** CONSTRUCTORS ** \\

		/// <summary>
		/// Constructor that works with preconfigured profiles.
		/// </summary>
		/// <param name="profileRequest">Name of the profile.</param>
		/// <param name="region">Region to access.</param>
		public ModuleAWSBOXS3([Optional] string profileRequest, RegionEndpoint region)
		{
			try
			{
				// Access using the credentials file
				var chain = new CredentialProfileStoreChain();
				if (chain.TryGetAWSCredentials(profileRequest, out AWSCredentials awsCredentials))
				{
					_log.Information("AWS - Creating new client in {Region}.", region);
					s3Client = new AmazonS3Client(awsCredentials, region);
					s3Clients.Add(s3Client);
					activeProfile = profileRequest;
				}
			}
			catch (AmazonS3Exception amazonS3Exception) { _log.Error(amazonS3Exception.Message); }
			catch (Exception exception) { _log.Error(exception.Message); }
		}

		/// <summary>
		/// Constructor for non-preconfigured access.
		/// </summary>
		/// <param name="credentials">AccessKey and SecretKey credentials.</param>
		/// <param name="region">Region to access.</param>
		public ModuleAWSBOXS3(BasicAWSCredentials credentials, RegionEndpoint region)
		{
			try
			{
				_log.Information("AWS - Creating new client in {Region}.", region);
				s3Client = new AmazonS3Client(credentials, region);
				s3Clients.Add(s3Client);
				// TODO: Warning, if you access from this constructor, ChangeRegion will not work properly.
			}
			catch (AmazonS3Exception amazonS3Exception) { _log.Error(amazonS3Exception.Message); }
			catch (Exception exception) { _log.Error(exception.Message); }
		}

		// ** CONSTRUCTORS END ** \\


		// ** INTERNAL FUNCTION ** \\

		/// <summary>
		/// Searches the list of clients for the client associated with the specified region.
		/// </summary>
		/// <param name="region">The region to search for.</param>
		/// <returns>AWS S3 client associated with the region.</returns>
		private AmazonS3Client GetClientByRegion(RegionEndpoint region)
		{
			_log.Debug("AWS - Requested client associated with region {region}", region);
			foreach (var client in s3Clients)
			{
				if (client.Config.RegionEndpoint == region)
				{
					_log.Debug("AWS - Client located successfully");
					return client;
				}
			}
			_log.Debug("AWS - Client not found");
			return null;
		}

		/// <summary>
		/// Gets the client associated with the specified region, with fallback behavior.
		/// </summary>
		/// <param name="region">The region to retrieve the client for.</param>
		/// <returns>The Amazon S3 client for the specified region, or the existing client with fallback behavior.</returns>
		private AmazonS3Client GetClientByRegionForced(RegionEndpoint region)
		{
			_log.Debug("AWS - Requested client associated with the {region} region.", region);
			AmazonS3Client response = GetClientByRegion(region);
			if (response == null)
			{
				_log.Debug("AWS - Client for the {region} region not found.", region);
				AddNewRegion(region);
				ChangeRegion(region);
				return s3Client;
			}
			return response;
		}

		/// <summary>
		/// Checks if the bucket exists.
		/// </summary>
		/// <param name="bucketName">The name of the bucket.</param>
		/// <returns>True if the bucket exists; otherwise, false.</returns>
		private bool CheckIfBucketExist(string bucketName)
		{
			try
			{
				return AmazonS3Util.DoesS3BucketExistV2(s3Client, bucketName);
			}
			catch (AmazonS3Exception amazonS3Exception) { _log.Error(amazonS3Exception.Message); }
			catch (Exception exception) { _log.Error(exception.Message); }
			return false;
		}

		/// <summary>
		/// Searches the list of clients for the client associated with the bucket's region.
		/// </summary>
		/// <param name="bucketName">The name of the bucket.</param>
		/// <returns>The AWS S3 client associated with the bucket's region.</returns>
		private AmazonS3Client GetClientByBucketName(string bucketName)
		{
			try
			{
				bool exist = CheckIfBucketExist(bucketName);
				if (exist)
				{
					GetBucketLocationRequest getBucketLocationRequest = new GetBucketLocationRequest { BucketName = bucketName };
					GetBucketLocationResponse getBucketLocationResponse = s3Client.GetBucketLocation(getBucketLocationRequest);
					return GetClientByRegion(CastS3RegionToRegionEndpoint(getBucketLocationResponse.Location));
				}
			}
			catch (AmazonS3Exception amazonS3Exception) { _log.Error(amazonS3Exception.Message); }
			catch (Exception exception) { _log.Error(exception.Message); }
			return null;
		}

		/// <summary>
		/// Checks if a session with the specified region already exists.
		/// </summary>
		/// <param name="region">The region to check.</param>
		/// <returns>True if a session with the region exists, false otherwise.</returns>
		private bool checkIfRegionExist(RegionEndpoint region)
		{
			foreach (AmazonS3Client client in s3Clients)
			{
				if (client.Config.RegionEndpoint == region)
					return true;
			}
			return false;
		}

		/// <summary>
		/// Function to cast S3Region to RegionEndpoint.
		/// <br>See <a href="https://docs.aws.amazon.com/sdkfornet/v3/apidocs/items/S3/TS3Region.html"/></br>
		/// </summary>
		/// <param name="regionName">The S3Region value.</param>
		/// <returns>The corresponding RegionEndpoint.</returns>
		private RegionEndpoint CastS3RegionToRegionEndpoint(S3Region regionName)
		{
			switch (S3Region.FindValue(regionName))
			{
				case "EU":
					return RegionEndpoint.EUWest1;
				default:
					return RegionEndpoint.GetBySystemName(regionName);
			}
		}

		// ** INTERNAL FUNCTION END ** \\


		// ** PUBLIC FUNCTION ** \\

		/// <summary>
		/// Changes the region of the AWS client.
		/// </summary>
		/// <param name="region">The region to set.</param>
		/// <returns>True if the region change was successful; otherwise, false.</returns>
		public bool ChangeRegion(RegionEndpoint region)
		{
			_log.Information("AWS - Requesting client from {Region}.", region);
			AmazonS3Client newClient = GetClientByRegion(region);
			if (newClient == null)
				return false;
			s3Client = newClient;
			return true;
		}


		/// <summary>
		/// Adds a new region to the AWS client if it doesn't already exist.
		/// </summary>
		/// <param name="region">The region to add.</param>
		public void AddNewRegion(RegionEndpoint region)
		{
			if (!checkIfRegionExist(region))
			{
				// Access via credentials file
				AWSCredentials awsCredentials;
				var chain = new CredentialProfileStoreChain();
				if (chain.TryGetAWSCredentials(activeProfile, out awsCredentials))
				{
					_log.Information("AWS - Creating new client in {Region}.", region);
					s3Clients.Add(new AmazonS3Client(awsCredentials, region));
				}
			}
		}

		/// <summary>
		/// Gets a list of credential profiles.
		/// </summary>
		/// <returns>The list of credential profiles.</returns>
		public List<CredentialProfile> GetListCredentials()
		{
			_log.Information("Requesting credentials");
			return new CredentialProfileStoreChain().ListProfiles();
		}

		/// <summary>
		/// Returns a list of buckets owned by the S3 client.
		/// </summary>
		/// <returns>A list of buckets.</returns>
		public List<S3Bucket> MyListBuckets()
		{
			try
			{
				ListBucketsResponse response = new ListBucketsResponse();
				_log.Information("AWS - Requesting list of buckets.");
				response = s3Client.ListBuckets();
				if (response != null)
					return response.Buckets;
				return null;
			}
			catch (AmazonS3Exception amazonS3Exception) { _log.Error(amazonS3Exception.Message); }
			catch (Exception exception) { _log.Error(exception.Message); }
			return null;
		}

		/// <summary>
		/// Returns a list of buckets owned by the specified S3 client, filtered by region.
		/// </summary>
		/// <param name="region">The region to filter the buckets.</param>
		/// <returns>A list of buckets filtered by region.</returns>
		public List<S3Bucket> MyListBucketsByRegion(RegionEndpoint region)
		{
			try
			{
				_log.Information("AWS - Requesting list of buckets.");
				ListBucketsResponse listBucketsResponse = s3Client.ListBuckets();
				_log.Debug("Filtering buckets by region {region}", region);
				List<S3Bucket> lbucket = listBucketsResponse.Buckets;
				List<S3Bucket> response = new List<S3Bucket>();
				foreach (var bucket in lbucket)
				{
					GetBucketLocationRequest getBucketLocationRequest = new GetBucketLocationRequest { BucketName = bucket.BucketName };
					GetBucketLocationResponse getBucketLocationResponse = s3Client.GetBucketLocation(getBucketLocationRequest);
					if (CastS3RegionToRegionEndpoint(getBucketLocationResponse.Location) == region)
						response.Add(bucket);
				}
				return response;
			}
			catch (AmazonS3Exception amazonS3Exception) { _log.Error(amazonS3Exception.Message); }
			catch (Exception exception) { _log.Error(exception.Message); }
			return null;
		}

		/// <summary>
		/// Creates a new bucket with the specified name and region.
		/// </summary>
		/// <param name="bucketName">The name of the bucket to create.</param>
		/// <param name="region">The region where the bucket will be created.</param>
		/// <returns>True if the bucket creation was successful; otherwise, false.</returns>
		public bool CreateBucket(string bucketName, RegionEndpoint region)
		{
			try
			{
				_log.Information("AWS - Creating new bucket {name} in {region}.", bucketName, region);
				return (GetClientByRegionForced(region).PutBucket(new PutBucketRequest
				{
					BucketName = bucketName,
					UseClientRegion = true,
				})).HttpStatusCode == HttpStatusCode.OK;
			}
			catch (AmazonS3Exception amazonS3Exception) { _log.Error(amazonS3Exception.Message); }
			catch (Exception exception) { _log.Error(exception.Message); }
			return false;
		}


		/// <summary>
		/// Retrieves a list of objects in the specified bucket.
		/// </summary>
		/// <param name="bucketName">The name of the bucket.</param>
		/// <returns>A list of S3Object.</returns>
		public List<S3Object> ListBucketContent(string bucketName)
		{
			try
			{
				_log.Information("AWS - Requesting content of bucket {bucketName}.", bucketName);
				activeBucketName = bucketName;
				return GetClientByBucketName(bucketName).ListObjectsV2(new ListObjectsV2Request { BucketName = bucketName, MaxKeys = 5 }).S3Objects;
			}
			catch (AmazonS3Exception amazonS3Exception) { _log.Error(amazonS3Exception.Message); }
			catch (Exception exception) { _log.Error(exception.Message); }
			return null;
		}

		/// <summary>
		/// Retrieves a list of object versions in the specified bucket for a given file name.
		/// </summary>
		/// <param name="bucketName">The name of the bucket.</param>
		/// <param name="fileName">The name of the file.</param>
		/// <returns>A list of S3ObjectVersion.</returns>
		public List<S3ObjectVersion> ListObjectVersions(string bucketName, string fileName)
		{
			try
			{
				_log.Information("AWS - Requesting information of versions for file {fileName}.", fileName);
				ListVersionsResponse listVersionsResponse = GetClientByBucketName(bucketName).ListVersions(new ListVersionsRequest
				{
					BucketName = bucketName,
					MaxKeys = 2
				});
				_log.Debug("AWS - Filtering version list for file {fileName}", fileName);
				List<S3ObjectVersion> lS3ObjectVersions = listVersionsResponse.Versions;
				List<S3ObjectVersion> response = new List<S3ObjectVersion>();
				int versionsNumber = 0;
				foreach (var s3ObjectVersions in lS3ObjectVersions)
				{
					if (s3ObjectVersions.Key.Equals(fileName))
					{
						response.Add(s3ObjectVersions);
						versionsNumber++;
					}
				}
				_log.Debug("AWS - Version listing completed - Total versions: {num}", versionsNumber);
				return response;
			}
			catch (AmazonS3Exception amazonS3Exception) { _log.Error(amazonS3Exception.Message); }
			catch (Exception exception) { _log.Error(exception.Message); }
			return null;
		}

		/// <summary>
		/// Uploads a file to a specified bucket.
		/// </summary>
		/// <param name="bucketName">The name of the bucket.</param>
		/// <param name="objectName">The name of the object.</param>
		/// <param name="filePath">The path to the file.</param>
		/// <returns>True if the upload process is successful, otherwise false.</returns>
		public async Task<bool> UploadFileAsync(string bucketName, string objectName, string filePath)
		{
			try
			{
				_log.Information("AWS - Uploading file {fileName} to bucket {bucketName}.", objectName, bucketName);
				return (await GetClientByBucketName(bucketName).PutObjectAsync(new PutObjectRequest
				{
					BucketName = bucketName,
					Key = objectName,
					FilePath = filePath
				})).HttpStatusCode == HttpStatusCode.OK;
			}
			catch (AmazonS3Exception amazonS3Exception) { _log.Error(amazonS3Exception.Message); }
			catch (Exception exception) { _log.Error(exception.Message); }
			return false;
		}

		/// <summary>
		/// Downloads an object from a specified bucket to a specified path.
		/// </summary>
		/// <param name="bucketName">The name of the bucket.</param>
		/// <param name="objectName">The name of the object.</param>
		/// <param name="filePath">The path where the object will be downloaded.</param>
		/// <param name="version">The version of the object (optional).</param>
		/// <returns>True if the download process is successful, otherwise false.</returns>
		public async Task<bool> DownloadObjectFromBucketAsync(string bucketName, string objectName, string filePath, [Optional] string version)
		{
			try
			{
				_log.Information("AWS - Downloading file {fileName} from bucket {bucketName}.", objectName, bucketName);
				GetObjectResponse response = await GetClientByBucketName(bucketName).GetObjectAsync(new GetObjectRequest
				{
					BucketName = bucketName,
					Key = objectName,
					VersionId = version
				});
				await response.WriteResponseStreamToFileAsync($"{filePath}\\{objectName}", true, CancellationToken.None);
				return response.HttpStatusCode == HttpStatusCode.OK;
			}
			catch (AmazonS3Exception amazonS3Exception) { _log.Error(amazonS3Exception.Message); }
			catch (Exception exception) { _log.Error(exception.Message); }
			return false;
		}


		/// <summary>
		/// Deletes an object from a specified bucket.
		/// </summary>
		/// <param name="bucketName">The name of the bucket.</param>
		/// <param name="objectName">The name of the object.</param>
		/// <param name="version">The version of the object (optional).</param>
		/// <returns>True if the deletion process is successful, otherwise false.</returns>
		public bool DeleteObjectFromBucket(string bucketName, string objectName, [Optional] string version)
		{
			try
			{
				_log.Information("AWS - Deleting file {fileName} from bucket {bucketName}.", objectName, bucketName);
				return GetClientByBucketName(bucketName).DeleteObject(new DeleteObjectRequest
				{
					BucketName = bucketName,
					Key = objectName,
					VersionId = version
				}).HttpStatusCode == HttpStatusCode.OK;
			}
			catch (AmazonS3Exception amazonS3Exception) { _log.Error(amazonS3Exception.Message); }
			catch (Exception exception) { _log.Error(exception.Message); }
			return false;
		}

		/// <summary>
		/// Deletes a bucket from the client.
		/// </summary>
		/// <param name="bucketName">The name of the bucket.</param>
		/// <returns>True if the deletion process is successful, otherwise false.</returns>
		public bool DeleteBucket(string bucketName)
		{
			try
			{
				_log.Information("AWS - Deleting bucket {bucketName}.", bucketName);
				activeBucketName = string.Empty;
				return GetClientByBucketName(bucketName).DeleteBucket(new DeleteBucketRequest
				{
					BucketName = bucketName
				}).HttpStatusCode == HttpStatusCode.OK;
			}
			catch (AmazonS3Exception amazonS3Exception) { _log.Error(amazonS3Exception.Message); }
			catch (Exception exception) { _log.Error(exception.Message); }
			return false;
		}

		/// <summary>
		/// Restores a versioned object from a specified bucket.
		/// </summary>
		/// <param name="bucketName">The name of the bucket.</param>
		/// <param name="objectName">The name of the object.</param>
		/// <param name="version">The version of the object (optional).</param>
		/// <returns>True if the restoration process is successful, otherwise false.</returns>
		public bool RestoreObjectVersionedBucket(string bucketName, string objectName, [Optional] string version)
		{
			try
			{
				_log.Information("AWS - Restoring file {fileName} from bucket {bucketName}.", objectName, bucketName);
				return GetClientByBucketName(bucketName).RestoreObject(new RestoreObjectRequest
				{
					BucketName = bucketName,
					Key = objectName,
					VersionId = version
				}).HttpStatusCode == HttpStatusCode.OK;
			}
			catch (AmazonS3Exception amazonS3Exception)
			{
				_log.Error(amazonS3Exception.ErrorCode);
				_log.Debug(amazonS3Exception.Message);
			}
			catch (Exception exception) { _log.Error(exception.Message); }
			return false;
		}

		// ** PUBLIC FUNCTION END ** \\
	}

}
