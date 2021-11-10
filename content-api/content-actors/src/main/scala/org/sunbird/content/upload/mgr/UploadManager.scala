package org.sunbird.content.upload.mgr

import java.io.File
import java.util

import org.apache.commons.lang3.StringUtils
import org.sunbird.models.UploadParams
import org.sunbird.common.Platform
import org.sunbird.common.dto.{Request, Response, ResponseHandler}
import org.sunbird.common.exception.{ClientException, ResponseCode}
import org.sunbird.content.util.ContentConstants
import org.sunbird.graph.OntologyEngineContext
import org.sunbird.graph.dac.model.Node
import org.sunbird.graph.nodes.DataNode
import org.sunbird.mimetype.factory.MimeTypeManagerFactory
import org.sunbird.telemetry.util.LogTelemetryEventUtil
import org.sunbird.telemetry.logger.TelemetryManager

import scala.collection.JavaConversions.mapAsJavaMap
import scala.concurrent.{ExecutionContext, Future}
import org.sunbird.kafka.client.KafkaClient

import scala.collection.Map

object UploadManager {

	private val MEDIA_TYPE_LIST = List("image", "video")
	private val kfClient = new KafkaClient
	private val CONTENT_ARTIFACT_ONLINE_SIZE: Double = Platform.getDouble("content.artifact.size.for_online", 209715200.asInstanceOf[Double])


	def upload(request: Request, node: Node)(implicit oec: OntologyEngineContext, ec: ExecutionContext): Future[Response] = {
		val identifier: String = node.getIdentifier
		val fileUrl: String = request.getRequest.getOrDefault("fileUrl", "").asInstanceOf[String]
		val file = request.getRequest.get("file").asInstanceOf[File]
		val reqFilePath: String = request.getRequest.getOrDefault("filePath", "").asInstanceOf[String].replaceAll("^/+|/+$", "")
		val filePath = if(StringUtils.isBlank(reqFilePath)) None else Option(reqFilePath)
		val mimeType = node.getMetadata().getOrDefault("mimeType", "").asInstanceOf[String]
		val mediaType = node.getMetadata.getOrDefault("mediaType", "").asInstanceOf[String]
		val mgr = MimeTypeManagerFactory.getManager(node.getObjectType, mimeType)
		val params: UploadParams = request.getContext.get("params").asInstanceOf[UploadParams]
		println("upload->mimetype:" + mimeType);
		println("upload->mediaType:" + mediaType);
		println("upload->filePath:" + filePath);
		val uploadFuture: Future[Map[String, AnyRef]] = if (StringUtils.isNotBlank(fileUrl)) mgr.upload(identifier, node, fileUrl, filePath, params) else mgr.upload(identifier, node, file, filePath, params)
		println("upload->uploadFuture:" + uploadFuture);
		uploadFuture.map(result => {
			if(filePath.isDefined){
				TelemetryManager.info("upload->filePath defined:");
				updateNode(request, node.getIdentifier, mediaType, node.getObjectType, result + (ContentConstants.ARTIFACT_BASE_PATH -> filePath.get))
			}else{
				TelemetryManager.info("upload->filePath else part:");
				updateNode(request, node.getIdentifier, mediaType, node.getObjectType, result)
			}
		}).flatMap(f => f)
	}

	def updateNode(request: Request, identifier: String, mediaType: String, objectType: String, result: Map[String, AnyRef])(implicit oec: OntologyEngineContext, ec: ExecutionContext): Future[Response] = {
		val updatedResult = result - "identifier"
		val artifactUrl = updatedResult.getOrElse("artifactUrl", "").asInstanceOf[String]
		println("updateNode:updatedResult:" + updatedResult)
		println("updateNode:artifactUrl:" + artifactUrl)
		val size: Double = updatedResult.getOrElse("size", 0.asInstanceOf[Double]).asInstanceOf[Double]
		println("updateNode:size:" + size)
		if (StringUtils.isNotBlank(artifactUrl)) {
			val updateReq = new Request(request)
			println("updateNode->if part->updateReq" + updateReq)
			updateReq.getContext().put("identifier", identifier)
			updateReq.getRequest.putAll(mapAsJavaMap(updatedResult))
			if( size > CONTENT_ARTIFACT_ONLINE_SIZE)
				updateReq.put("contentDisposition", "online-only")
			if (StringUtils.equalsIgnoreCase("Asset", objectType) && MEDIA_TYPE_LIST.contains(mediaType))
				updateReq.put("status", "Processing")
			println("updateNode:One:" + updateReq)
			DataNode.update(updateReq).map(node => {
				if (StringUtils.equalsIgnoreCase("Asset", objectType) && MEDIA_TYPE_LIST.contains(mediaType) && null != node)
					pushInstructionEvent(identifier, node)
				getUploadResponse(node)
			})
			
			println("updateNode:Complete:")
		} else {
			println("updateNode:Else part:" + artifactUrl);
			Future {
				ResponseHandler.ERROR(ResponseCode.SERVER_ERROR, "ERR_UPLOAD_FILE", "Something Went Wrong While Processing Your Request.")
			}
		}
	}

	def getUploadResponse(node: Node)(implicit ec: ExecutionContext): Response = {
		val response: Response = ResponseHandler.OK
		val id = node.getIdentifier.replace(".img", "")
		val url = node.getMetadata.get("artifactUrl").asInstanceOf[String]
		response.put("node_id", id)
		response.put("identifier", id)
		response.put("artifactUrl", url)
		response.put("content_url", url)
		response.put("versionKey", node.getMetadata.get("versionKey"))
		println("getUploadResponse:::upload response:: " + response)
		response
	}

	@throws[Exception]
	private def pushInstructionEvent(identifier: String, node: Node): Unit = {
		val actor: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
		val context: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
		val objectData: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
		val edata: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
		generateInstructionEventMetadata(actor, context, objectData, edata, node, identifier)
		val beJobRequestEvent: String = LogTelemetryEventUtil.logInstructionEvent(actor, context, objectData, edata)
		val topic: String = Platform.getString("kafka.topics.instruction","sunbirddev.learning.job.request")
		println("inside pushInstructionEvent::" +node)
		if (StringUtils.isBlank(beJobRequestEvent)) throw new ClientException("BE_JOB_REQUEST_EXCEPTION", "Event is not generated properly.")
		kfClient.send(beJobRequestEvent, topic)
	}

	private def generateInstructionEventMetadata(actor: util.Map[String, AnyRef], context: util.Map[String, AnyRef], objectData: util.Map[String, AnyRef], edata: util.Map[String, AnyRef], node: Node, identifier: String): Unit = {
		val metadata: util.Map[String, AnyRef] = node.getMetadata
		actor.put("id", "Asset Enrichment Samza Job")
		actor.put("type", "System")
		context.put("channel", metadata.get("channel"))
		context.put("pdata", new util.HashMap[String, AnyRef]() {{
				put("id", "org.sunbird.platform")
				put("ver", "1.0")
			}})
		if (Platform.config.hasPath("cloud_storage.env")) {
			val env: String = Platform.getString("cloud_storage.env", "dev")
			context.put("env", env)
		}
		objectData.put("id", identifier)
		objectData.put("ver", metadata.get("versionKey"))
		edata.put("action", "assetenrichment")
		edata.put("status", metadata.get("status"))
		edata.put("mediaType", metadata.get("mediaType"))
		edata.put("objectType", node.getObjectType)
	}
}
