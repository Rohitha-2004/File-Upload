import React, { useState } from 'react';
import { Upload, message, Button, Modal, Spin, Radio } from 'antd';
import { CloudUploadOutlined, CheckCircleOutlined } from '@ant-design/icons';
import axios from 'axios';
import styled from 'styled-components';

const { Dragger } = Upload;

const FileUpload = () => {
  const [selectedTable, setSelectedTable] = useState(null);
  const [openConfirmModal, setOpenConfirmModal] = useState(false);
  const [fileList, setFileList] = useState([]);
  const [actionType, setActionType] = useState('append'); // Default action is append/insert
  const [loading, setLoading] = useState(false); // State for loading spinner

  const handleTableChange = (e) => {
    setSelectedTable(e.target.value);
  };

  const handleActionTypeChange = (e) => {
    setActionType(e.target.value);
  };

  const handleUpload = () => {
    if (!selectedTable || fileList.length === 0) {
      message.error('Please select a table and upload a file.');
      return;
    }

    setLoading(true); // Show loading spinner on confirm

    const formData = new FormData();
    formData.append('table', selectedTable); // Append selected table
    formData.append('actionType', actionType); // Append action type

    fileList.forEach(file => {
      formData.append('file', file);
    });

    axios.post('http://localhost:5000/api/uploadFile', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    })
      .then(response => {
        message.success(response.data.message || 'File uploaded successfully.');
        setOpenConfirmModal(false);
        setFileList([]);
      })
      .catch(error => {
        console.error('Upload error:', error);
        message.error('Failed to upload file.');
      })
      .finally(() => {
        setLoading(false); // Hide loading spinner after response
        setOpenConfirmModal(false);
      });
  };

  const props = {
    onRemove: file => {
      const index = fileList.indexOf(file);
      const newFileList = fileList.slice();
      newFileList.splice(index, 1);
      setFileList(newFileList);
    },
    beforeUpload: file => {
      setFileList([file]); // Only allow one file at a time
      return false;
    },
    fileList,
  };

  return (
    <Container>
      <Title>Select table</Title>
      <Radio.Group onChange={handleTableChange} style={{ marginBottom: '16px' }}>
        <Radio value="service_bnow_type">Service BNow Type</Radio>
        <Radio value="colleague_delight_inc_data">Colleague Delight Inc Data</Radio>
        <Radio value="assignment_group_poc">Assignment Group POC</Radio>
      </Radio.Group>
      <Title>Select Action</Title>
      <Radio.Group onChange={handleActionTypeChange} value={actionType} style={{ marginBottom: '16px' }}>
        <Radio value="append">Append</Radio>
        <Radio value="truncate_insert">Truncate and Insert</Radio>
      </Radio.Group>
      <Dragger {...props} className="ant-upload">
        {fileList.length === 0 ? (
          <>
            <p className="ant-upload-drag-icon">
              <CloudUploadOutlined style={{ fontSize: '36px', color: '#1890ff' }} />
            </p>
            <p className="ant-upload-text" style={{ fontSize: '16px', color: '#1890ff' }}>
              Click or drag file to this area to upload
            </p>
          </>
        ) : (
          <p className="ant-upload-drag-icon">
            <CheckCircleOutlined style={{ fontSize: '36px', color: '#52c41a' }} />
          </p>
        )}
      </Dragger>
      <div style={{ textAlign: 'center', marginTop: '16px' }}>
        <Button
          type="primary"
          onClick={() => setOpenConfirmModal(true)}
          style={{ marginTop: '16px' }}
          disabled={loading}
        >
          Confirm
        </Button>
        <Modal
          title={`Confirm Data Action on ${selectedTable}`}
          visible={openConfirmModal}
          onOk={handleUpload}
          onCancel={() => setOpenConfirmModal(false)}
          okText="Confirm"
          cancelText="Cancel"
        >
          {loading ? (
            <div style={{ textAlign: 'center' }}>
              <Spin size="large" />
              <p>Uploading...</p>
            </div>
          ) : (
            <p>Are you sure you want to {actionType === 'truncate_insert' ? 'truncate and insert' : 'append'} data for {selectedTable}?</p>
          )}
        </Modal>
      </div>
    </Container>
  );
};

const Container = styled.div`
  padding: 20px;
  min-height: 60vh;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: flex-start;
  margin-top: 40px;
  position: relative;
  background-color: #f0f2f5; /* Set background to a light gray color */
  
  .ant-upload {
    width: 300px;
    height: 300px;
    border: 1px dashed #1890ff;
    background: #ffffff; /* Set the background to white */
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
  }
`;

const Title = styled.h2`
  margin-bottom: 16px;
  color: #1890ff; /* Set the title color to blue */
  text-align: center; /* Center the title */
`;

export default FileUpload;
