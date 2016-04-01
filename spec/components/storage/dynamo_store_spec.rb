require 'spec_helper'

require 'aws-sdk'

describe Rack::MiniProfiler::DynamoStore do
  let(:table_name)  { 'FakeRackMiniProfiler' }
  let(:client) { Aws::DynamoDB::Client.new(region: "us-east-1", endpoint: 'http://localhost:8000') }

  describe 'initialization' do
    it 'creates a table if one does not exist' do
      client.delete_table({table_name: table_name}) rescue nil

      expect{ client.describe_table({table_name: table_name}) }.to raise_error /non-existent table/

      Rack::MiniProfiler::DynamoStore.new({client: client, table_name: table_name})

      expect(client.describe_table({table_name: table_name}).successful?).to be true
    end

    it 'does not throw exception if the table already exists' do
      client.delete_table({table_name: table_name}) rescue nil

      Rack::MiniProfiler::DynamoStore.new({client: client, table_name: table_name})
      expect(client.describe_table({table_name: table_name}).successful?).to be true
      expect{Rack::MiniProfiler::DynamoStore.new({client: client, table_name: table_name})}.not_to raise_error
    end

  end

  context 'page struct' do
    let(:store)  { Rack::MiniProfiler::DynamoStore.new(client: client) }

    before do
      store.
    end
    describe 'storage' do

      it 'can store a PageStruct and retrieve it' do
        page_struct = Rack::MiniProfiler::TimerStruct::Page.new({})
        page_struct[:id] = "XYZ"
        page_struct[:random] = "random"
        store.save(page_struct)
        page_struct = store.load("XYZ")
        page_struct[:random].should == "random"
        page_struct[:id].should == "XYZ"
      end

      it 'can list unviewed items for a user' do
        store.set_unviewed('a', 'XYZ')
        store.set_unviewed('a', 'ABC')
        require 'byebug'
        byebug
        store.get_unviewed_ids('a').length.should == 2
        puts 'asdfasdf'
        #store.get_unviewed_ids('a').include?('XYZ').should be_true
        #store.get_unviewed_ids('a').include?('ABC').should be_true
      end

      it 'can set an item to viewed once it is unviewed' do
        store.set_unviewed('a', 'XYZ')
        store.set_unviewed('a', 'ABC')
        store.set_viewed('a', 'XYZ')
        #store.get_unviewed_ids('a').should == ['ABC']
      end

    end

  end

end
